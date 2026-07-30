Optional local-only test fixtures.

- `sample_reportTable.xlsx` (not committed): a real Vahan-downloaded report, used by
  `tests.test_schema_regression.PipelineSchemaRegressionTests.test_real_sample_workbook_headers_match_shared_mapping`
  to catch Vahan changing its actual column headers. The test skips gracefully when this
  file is absent. Drop any real `reportTable.xlsx` download here (renamed) to exercise the
  check locally.
