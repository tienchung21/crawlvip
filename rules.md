## HTTP Rules

- ALWAYS use curl_cffi
- Always explain step-by-step in Vietnamese
- Before coding, explain:
  - problem
  - approach
  - why this solution
## Test File Rules

- ALL test files must be placed inside /test folder
- NEVER create test files outside /test

- When testing:
  - SQL -> /test/sql/
  - API -> /test/api/
  - scraping -> /test/crawl/

- If a new test file is needed:
  - reuse existing file if possible
  - otherwise create inside /test

- After testing:
  - do not leave temporary or duplicate files outside /test