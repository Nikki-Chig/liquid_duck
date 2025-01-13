# Liquid Duck - Multi-Level Beverage Distribution Analysis
## 1. Introduction
This repository demonstrates a multi-level beverage distribution analysis using Python and DuckDB. The goal is to showcase:

- SQL transformations like grouping sets, common table expressions.
- Multiple derived tables that represent different aggregation levels.
- A single-file database approach (DuckDB), enabling easy local development and testing.
- An orchestrator script (main.py) that runs everything in one shot.

## 2. Project Structure
Below is a  layout of the key directories and files:
beverage_distribution_analysis/
├── data/                      # CSV or generated data (if any)
├── scripts/                   # Ad hoc scripts (optional)
├── src/
│   ├── db/
│   │   └── duckdb_setup.py    # Creates base tables (product, customer, sales) & populates with Faker
│   ├── analysis/
│   │   ├── supplier_metrics.py
│   │   ├── supplier_metrics_pivot_by_quarter.py
│   │   ├── customer_supplier_metrics.py
│   │   ├── union_metrics.py
│   │   └── pivot_unpivot_union_metrics.py
│   └── main.py                # Orchestrator: calls all scripts in sequence
├── tests/
│   └── test_setup.py          # Pytest script
├── requirements.txt           # Python dependencies
├── README.md                  # This technical documentation
└── ...

**Key Points**
- duckdb_setup.py initializes and populates the base tables (product, customer, sales).
- Each derived table script (e.g. supplier_metrics.py) creates new aggregated tables.
- main.py calls them all in sequence, so you only need one command to run the entire flow.
- tests/ can hold unit or integration tests (using pytest).

## 3. Features
- **Base Data Generation**
    - Uses Faker to generate synthetic data for product, customer, and sales tables in DuckDB.

- **Advanced SQL**
    - Grouping Sets to aggregate data at multiple hierarchy levels (e.g., supplier, brand, family, quarter).

- **Orchestrated Execution**
    - main.py script ensures you can run all table creation steps in a single command.

- **Flexible Architecture**
    - Each script can be run standalone or as part of the orchestrator for debugging or modular development.

- **Testing with Pytest**
    - Example tests in the tests/ folder illustrate how to verify table creation and data integrity.

## 4. Installation & Setup
1. **Clone the Repository**
git clone https://github.com/Nikki-Chig/liquid_duck.git
cd beverage_distribution_analysis

2. **Create & Activate a Virtual Environment (recommended)**
python3 -m venv venv
source venv/bin/activate   # Linux/macOS
# or
venv\Scripts\activate      # Windows

3. **Install Dependencies**
pip install -r requirements.txt

## 5. Running the Orchestrator Script
To create and populate all tables, run:
python src/main.py

**Note:** If you see an import error (ModuleNotFoundError) for db or analysis, ensure you’re in the project root folder and that your folder structure matches the import statements.

**What Happens**

1. duckdb_setup.init_db_and_populate() creates the base tables in beverage_analysis.db.
2. Each derived table script is called in turn (supplier_metrics, customer_supplier_metrics, union_metrics, etc.)
3. A final .db file is generated/updated locally with all aggregated tables ready for query.

## 5. Data Flow & Scripts
**Base Tables**
- src/db/duckdb_setup.py
    - Creates product, customer, and sales tables in beverage_analysis.db.
    - Uses Faker to generate synthetic data (e.g. random suppliers, brands, customers).

**Derived Tables**
1. supplier_metrics.py
    - Aggregates data by (supplier, brand, family, quarter) using grouping sets.

2. supplier_metrics_pivot_by_quarter.py
    - Demonstrates pivoting to transform data so that each quarter becomes a separate column.

3. customer_supplier_metrics.py
    - Summarizes sales by (supplier, customer_type).

4. union_metrics.py
    - Unions multiple aggregated tables and applies grouping sets again for further summarization.

5. pivot_unpivot_union_metrics.py (optional)
    - Showcases pivot and unpivot transformations on the unioned data.

6. Orchestration (main.py)
- src/main.py
    - Imports each script in the correct order.
    - Runs the base population first, then derived tables.
    - Simplifies the entire workflow into one command.

## 6. Testing
1. Pytest
    - Run from the project root: python -m pytest tests
    - Example test (test_setup.py) checks if base tables (product, customer, sales) exist and contain rows.
2. Add More Tests
    - You can add tests for each derived table to ensure they were created successfully and contain data.

## 7. Roadmap / Future Enhancements
- Docker Compose: Could automate multi-container setups (e.g., Redis for streams or a web server).
- Orchestration Tools: Migrate from a simple main.py to more robust DAG managers like Apache Airflow or Prefect.
- More Data Validations: Use Pydantic or Great Expectations for data quality checks.
- CI/CD Integration: Add GitHub Actions or similar to automate tests on each pull request.    
