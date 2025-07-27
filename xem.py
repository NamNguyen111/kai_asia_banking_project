[2025-07-27, 07:37:15] INFO - [0m00:37:15  1 of 1 OK snapshotted snapshots.snp_branches ................................... [[32mINSERT 0 1[0m in 0.23s]: source="airflow.task.hooks.cosmos.hooks.subprocess.FullOutputSubprocessHook"
[2025-07-27, 07:37:15] INFO - [0m00:37:15: source="airflow.task.hooks.cosmos.hooks.subprocess.FullOutputSubprocessHook"
[2025-07-27, 07:37:15] INFO - [0m00:37:15  Finished running 1 snapshot in 0 hours 0 minutes and 0.36 seconds (0.36s).: source="airflow.task.hooks.cosmos.hooks.subprocess.FullOutputSubprocessHook"
[2025-07-27, 07:37:15] INFO - [0m00:37:15: source="airflow.task.hooks.cosmos.hooks.subprocess.FullOutputSubprocessHook"
[2025-07-27, 07:37:15] INFO - [0m00:37:15  [32mCompleted successfully[0m: source="airflow.task.hooks.cosmos.hooks.subprocess.FullOutputSubprocessHook"
[2025-07-27, 07:37:15] INFO - [0m00:37:15: source="airflow.task.hooks.cosmos.hooks.subprocess.FullOutputSubprocessHook"
[2025-07-27, 07:37:15] INFO - [0m00:37:15  Done. PASS=1 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=1: source="airflow.task.hooks.cosmos.hooks.subprocess.FullOutputSubprocessHook"
[2025-07-27, 07:37:15] INFO - [0m00:37:15  [[33mWARNING[0m][DeprecationsSummary]: Deprecated functionality: source="airflow.task.hooks.cosmos.hooks.subprocess.FullOutputSubprocessHook"
[2025-07-27, 07:37:15] INFO - Summary of encountered deprecations:: source="airflow.task.hooks.cosmos.hooks.subprocess.FullOutputSubprocessHook"
[2025-07-27, 07:37:15] INFO - - ModelParamUsageDeprecation: 1 occurrence: source="airflow.task.hooks.cosmos.hooks.subprocess.FullOutputSubprocessHook"
[2025-07-27, 07:37:15] INFO - To see all deprecation instances instead of just the first occurrence of each,: source="airflow.task.hooks.cosmos.hooks.subprocess.FullOutputSubprocessHook"
[2025-07-27, 07:37:15] INFO - run command again with the `--show-all-deprecations` flag. You may also need to: source="airflow.task.hooks.cosmos.hooks.subprocess.FullOutputSubprocessHook"
[2025-07-27, 07:37:15] INFO - run with `--no-partial-parse` as some deprecations are only encountered during: source="airflow.task.hooks.cosmos.hooks.subprocess.FullOutputSubprocessHook"
[2025-07-27, 07:37:15] INFO - parsing.: source="airflow.task.hooks.cosmos.hooks.subprocess.FullOutputSubprocessHook"
[2025-07-27, 07:37:17] INFO - Command exited with return code 0: source="airflow.task.hooks.cosmos.hooks.subprocess.FullOutputSubprocessHook"
[2025-07-27, 07:37:17] INFO - parsing.: source="cosmos.operators.base"
[2025-07-27, 07:37:17] WARNING - 
                Airflow 3.0.0 Asset (Dataset) URIs validation rules changed and OpenLineage URIs (standard used by Cosmos) are no longer accepted.
                Therefore, if using Cosmos with Airflow 3, the Airflow Asset (Dataset) URI is now <postgres://db:5432/db_banking/staging/stg_branches>.
                Before, with Airflow 2.x, the URI used to be <postgres://db:5432/db_banking.staging.stg_branches>.
                Please, change any DAGs that were scheduled using the old standard to the new one.
                : source="cosmos.operators.local"
[2025-07-27, 07:37:17] WARNING - 
                Airflow 3.0.0 Asset (Dataset) URIs validation rules changed and OpenLineage URIs (standard used by Cosmos) are no longer accepted.
                Therefore, if using Cosmos with Airflow 3, the Airflow Asset (Dataset) URI is now <postgres://db:5432/db_banking/snapshots/snp_branches>.
                Before, with Airflow 2.x, the URI used to be <postgres://db:5432/db_banking.snapshots.snp_branches>.
                Please, change any DAGs that were scheduled using the old standard to the new one.
                : source="cosmos.operators.local"
[2025-07-27, 07:37:17] INFO - Inlets: [Asset(name='postgres://db:5432/db_banking/staging/stg_branches', uri='postgres://db:5432/db_banking/staging/stg_branches', group='asset', extra={}, watchers=[])]: source="cosmos.operators.base"
[2025-07-27, 07:37:17] INFO - Outlets: [Asset(name='postgres://db:5432/db_banking/snapshots/snp_branches', uri='postgres://db:5432/db_banking/snapshots/snp_branches', group='asset', extra={}, watchers=[])]: source="cosmos.operators.base"
[2025-07-27, 07:37:17] INFO - Assigning outlets with DatasetAlias in Airflow 3: source="cosmos.operators.local"