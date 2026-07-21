# climate_dot_dbt

This dbt project contains the curated SQL models that sit on top of the raw SQL Server tables loaded by the scraper pipelines.

Current operating notes:

- [`models/curated`](/Users/monish/DataScraper_VahanParivahan/climate_dot_dbt/models/curated) holds the active EV models.
- `rto_wise_ev_data` and `oem_wise_ev_data` are part of the live production workflow today.
- `state_wise_ev_data` remains available for manual validation and future production use, but it is not currently run automatically from the monthly VM cron path.
- Materialization is defined in each model so incremental behavior is visible at the SQL layer.

Safe validation commands:

```bash
bash /Users/monish/DataScraper_VahanParivahan/ops/run_repo_checks.sh
RUN_DBT_PARSE=1 DBT_PROFILES_DIR=/path/to/dbt/profiles bash /Users/monish/DataScraper_VahanParivahan/ops/run_repo_checks.sh
```
