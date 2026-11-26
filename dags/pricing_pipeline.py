import logging
from collections import defaultdict
from datetime import timedelta
from decimal import Decimal

import pendulum
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook


logger = logging.getLogger(__name__)

API_URL = "https://fakestoreapi.com/products"
POSTGRES_CONN_ID = "postgres_analytics"


def notify_failure(context):
    logger.error(
        "[ALERTA] Falha na tarefa %s | Erro: %s",
        context["task_instance"].task_id,
        context.get("exception")
    )


def notify_retry(context):
    logger.warning(
        "[RETRY] Nova tentativa da tarefa %s",
        context["task_instance"].task_id
    )


def notify_success(context):
    logger.info(
        "[SUCESSO] Tarefa %s finalizada",
        context["task_instance"].task_id
    )


@dag(
    dag_id="shopbrasil_pricing_pipeline",
    description="Pipeline de métricas de preços por categoria",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(
        2026, 1, 1,
        tz="America/Sao_Paulo"
    ),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args={
        "owner": "data-team",
        "on_failure_callback": notify_failure
    },
    tags=["ecommerce", "pricing", "etl"]
)
def pricing_pipeline():

    @task(
        retries=3,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=10),
        on_failure_callback=notify_failure,
        on_retry_callback=notify_retry,
        on_success_callback=notify_success
    )
    def buscar_produtos():

        try:
            response = requests.get(
                API_URL,
                timeout=30
            )

            response.raise_for_status()

            produtos = response.json()

            if not isinstance(produtos, list):
                raise ValueError("Resposta inválida da API")

            if not produtos:
                raise ValueError("API retornou lista vazia")

            logger.info(
                "%s produtos encontrados",
                len(produtos)
            )

            return [
                {
                    "id": p["id"],
                    "price": p["price"],
                    "category": p["category"]
                }
                for p in produtos
            ]

        except Exception:
            logger.exception("Erro ao buscar produtos")
            raise

    @task
    def validar_produtos(produtos):

        ids = set()

        for produto in produtos:

            if not isinstance(produto["id"], int):
                raise ValueError("ID inválido")

            if produto["id"] in ids:
                raise ValueError("Produto duplicado na API")

            ids.add(produto["id"])

            preco = produto["price"]

            if (
                isinstance(preco, bool)
                or not isinstance(preco, (int, float))
            ):
                raise ValueError("Preço inválido")

            valor = Decimal(str(preco))

            if not valor.is_finite() or valor < 0:
                raise ValueError("Preço inválido")

            categoria = produto["category"]

            if not isinstance(categoria, str) or not categoria.strip():
                raise ValueError("Categoria inválida")

        return produtos

    @task
    def agrupar_categorias(produtos):

        categorias = defaultdict(list)

        for produto in produtos:

            categoria = produto["category"]

            categorias[categoria].append(
                produto["price"]
            )

        return [
            {
                "category": categoria,
                "prices": precos
            }
            for categoria, precos in categorias.items()
        ]

    @task(pool="ecommerce_pool")
    def calcular_metricas(grupo):

        precos = [
            Decimal(str(preco))
            for preco in grupo["prices"]
        ]

        media = sum(precos) / len(precos)

        return {
            "category": grupo["category"],
            "avg_price": str(round(media, 2)),
            "min_price": str(round(min(precos), 2)),
            "max_price": str(round(max(precos), 2)),
            "product_count": len(precos)
        }

    @task
    def consolidar_resultados(metricas):

        resultados = list(metricas)

        logger.info(
            "%s categorias processadas",
            len(resultados)
        )

        return resultados

    @task(retries=2)
    def salvar_metricas(resultados):

        context = get_current_context()

        run_id = context["run_id"]
        executed_at = context["dag_run"].start_date

        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        conn = hook.get_conn()

        try:
            with conn.cursor() as cursor:

                for metrica in resultados:

                    valores = (
                        metrica["category"],
                        metrica["avg_price"],
                        metrica["min_price"],
                        metrica["max_price"],
                        metrica["product_count"],
                        executed_at
                    )

                    cursor.execute(
                        """
                        INSERT INTO category_metrics (
                            category,
                            avg_price,
                            min_price,
                            max_price,
                            product_count,
                            updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)

                        ON CONFLICT (category)
                        DO UPDATE SET
                            avg_price = EXCLUDED.avg_price,
                            min_price = EXCLUDED.min_price,
                            max_price = EXCLUDED.max_price,
                            product_count = EXCLUDED.product_count,
                            updated_at = EXCLUDED.updated_at
                        WHERE category_metrics.updated_at
                            <= EXCLUDED.updated_at
                        """,
                        valores
                    )

                    cursor.execute(
                        """
                        INSERT INTO category_metrics_history (
                            category,
                            avg_price,
                            min_price,
                            max_price,
                            product_count,
                            executed_at,
                            run_id
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s)

                        ON CONFLICT (run_id, category)
                        DO NOTHING
                        """,
                        (*valores, run_id)
                    )

            conn.commit()

            logger.info("Métricas salvas com sucesso")

        except Exception:
            conn.rollback()
            logger.exception("Erro na persistência")
            raise

        finally:
            conn.close()

    with TaskGroup(group_id="ingestao"):

        produtos = buscar_produtos()

        produtos_validados = validar_produtos(produtos)

        categorias = agrupar_categorias(
            produtos_validados
        )

    with TaskGroup(group_id="analise"):

        metricas = calcular_metricas.expand(
            grupo=categorias
        )

        resultados = consolidar_resultados(metricas)

    salvar_metricas(resultados)


pricing_pipeline()