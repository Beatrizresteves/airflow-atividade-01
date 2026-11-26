# ShopBrasil — Pipeline de Preços por Categoria (Apache Airflow)

Pipeline que substitui o script agendado via cron da ShopBrasil. Todo dia às **06:00 (America/Sao_Paulo)** ele coleta os produtos da [FakeStore API](https://fakestoreapi.com/docs), calcula métricas de preço por categoria (média, mínimo, máximo e quantidade) e grava o resultado na base analítica em PostgreSQL.

## Como executar

Pré-requisito: Docker com Docker Compose.

```bash
docker compose up -d
```

A primeira subida leva alguns minutos (o serviço `airflow-init` migra o banco, cria o usuário e o pool `ecommerce_pool`).

| Serviço | Endereço | Credenciais |
|---|---|---|
| Airflow UI | http://localhost:8080 | `airflow` / `airflow` |
| PostgreSQL analítico | `localhost:5433`, banco `shopbrasil` | `ecommerce` / `ecommerce` |

A DAG `shopbrasil_pricing_pipeline` já nasce ativa (`is_paused_upon_creation=False`). Para uma execução imediata, use **Trigger DAG** na interface.

Consultar o resultado:

```bash
docker compose exec postgres-analytics psql -U ecommerce -d shopbrasil \
  -c "SELECT * FROM category_metrics ORDER BY category;"
```

## Fluxo do pipeline

```
┌─────────────── ingestao ───────────────┐   ┌──────────────── analise ────────────────┐
│ buscar_produtos → validar_produtos →    │   │ calcular_metricas ×N → consolidar_      │ → salvar_metricas
│                   agrupar_categorias    │ → │ (uma instância por      resultados      │
└─────────────────────────────────────────┘   │  categoria)                             │
                                              └─────────────────────────────────────────┘
        linear                                     fan-out              fan-in
```

| Task | Responsabilidade |
|---|---|
| `buscar_produtos` | Chama a API com timeout, valida a resposta e devolve só `id`, `price` e `category` (XCom pequeno). Task crítica: retry com backoff exponencial e os três callbacks. |
| `validar_produtos` | Rejeita IDs inválidos ou duplicados, preços não numéricos/negativos e categorias vazias antes de qualquer processamento. |
| `agrupar_categorias` | Agrupa preços por categoria sem nenhuma categoria fixa no código — uma categoria nova é processada automaticamente. |
| `calcular_metricas` | Mapeada dinamicamente com `.expand()`: uma instância por categoria, limitada pelo pool `ecommerce_pool` (2 slots). |
| `consolidar_resultados` | Fan-in: reúne as métricas de todas as categorias. |
| `salvar_metricas` | Grava via `PostgresHook` numa única transação (commit/rollback). |

## Requisitos atendidos

| Requisito | Onde |
|---|---|
| TaskFlow API (`@dag` / `@task`), dependências pela chamada das funções | `dags/pricing_pipeline.py` |
| XComs automáticos via `return`, apenas dados pequenos | todas as tasks |
| Topologias linear, fan-out e fan-in | `ingestao`, `calcular_metricas.expand()`, `consolidar_resultados` |
| Timezone `America/Sao_Paulo` com pendulum, `start_date`, `catchup=False` | decorator `@dag` |
| Retry com exponential backoff na ingestão | `buscar_produtos`: `retries=3`, `retry_exponential_backoff=True`, `max_retry_delay=10min` |
| `try/except` + `raise` para acionar o retry | `buscar_produtos` |
| Callbacks `on_failure`, `on_retry`, `on_success` na task crítica | `buscar_produtos` |
| Dynamic Task Mapping | `calcular_metricas.expand(grupo=categorias)` |
| Pool `ecommerce_pool` com 2 slots | criado no `airflow-init` (`docker-compose.yaml`) |
| Pelo menos 2 TaskGroups | `ingestao` e `analise` |
| `PostgresHook` + Connection do Airflow | conn `postgres_analytics`, definida via `AIRFLOW_CONN_POSTGRES_ANALYTICS` |
| Gravação idempotente | `ON CONFLICT (category) DO UPDATE` no snapshot; `ON CONFLICT (run_id, category) DO NOTHING` no histórico |
| Projeto dockerizado | `docker-compose.yaml` |
| *(Opcional)* Tabela de histórico em modo append | `category_metrics_history` |
| *(Opcional)* Alerta de falha | `notify_failure` em `default_args` (todas as tasks) |

## Idempotência

- **`category_metrics`** (snapshot): uma linha por categoria. Reprocessar atualiza a linha em vez de duplicar; o `WHERE updated_at <= EXCLUDED.updated_at` impede que uma execução antiga sobrescreva dados mais novos.
- **`category_metrics_history`** (append): cada run nova acrescenta uma linha por categoria; reprocessar a mesma run não duplica, graças à restrição `UNIQUE (run_id, category)`.

O schema das duas tabelas está em [`sql/init.sql`](sql/init.sql), aplicado automaticamente na criação do volume do PostgreSQL analítico.

## Estrutura

```
.
├── dags/
│   └── pricing_pipeline.py   
├── sql/
│   └── init.sql             
└── docker-compose.yaml        (CeleryExecutor) + Postgres analítico
```
