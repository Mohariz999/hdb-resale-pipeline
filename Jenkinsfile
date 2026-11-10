// CI pipeline for the HDB resale pipeline, run by the Jenkins server in jenkins/.
//
// The same checks as GitHub Actions (.github/workflows/ci.yml): lint the DAG, load a
// sample into a clean test database, build and test every dbt model, then build the
// Airflow image. Stages run in order, and the first failure stops the run.

pipeline {
    // Run on the Jenkins server itself. Its image already has Python, dbt, ruff, psql
    // and the Docker CLI (see jenkins/Dockerfile).
    agent any

    options {
        timeout(time: 30, unit: 'MINUTES')  // a hung step fails the run instead of blocking Jenkins
        disableConcurrentBuilds()           // runs share one test database, so one at a time
    }

    environment {
        PATH = "/opt/ci-venv/bin:${env.PATH}"

        // dbt's profiles.yml reads these to find the test database (the ci-postgres container).
        WAREHOUSE_HOST = 'ci-postgres'
        WAREHOUSE_PORT = '5432'

        // psql reads these, so the commands below don't repeat the connection details.
        PGHOST = 'ci-postgres'
        PGPORT = '5432'
        PGUSER = 'hdb'
        PGPASSWORD = 'hdb'
        PGDATABASE = 'hdb'
    }

    stages {
        stage('Lint') {
            steps {
                sh 'ruff check dags'
                sh 'ruff format --check dags'
            }
        }

        stage('Reset test database') {
            steps {
                // Start every run from an empty database, so nothing left over from the
                // last run can make a test pass (or fail) by accident.
                sh 'psql -v ON_ERROR_STOP=1 -c "DROP SCHEMA IF EXISTS raw, staging, marts, analytics CASCADE"'
            }
        }

        stage('Load sample data') {
            steps {
                // The same 130-row sample and raw table that GitHub Actions uses.
                sh 'psql -v ON_ERROR_STOP=1 -f ci/create_raw.sql'
            }
        }

        stage('dbt build') {
            steps {
                // Builds staging, the star schema and the mart, and runs all the data tests.
                dir('dbt') {
                    sh 'dbt build --profiles-dir .'
                }
            }
        }

        stage('Build Airflow image') {
            steps {
                // Uses the host's Docker engine through the mounted socket. Tagging with the
                // build number means every run produces its own, traceable image.
                sh 'docker build -t hdb-airflow:${BUILD_NUMBER} airflow'
            }
        }
    }

    post {
        always {
            // dbt's results file lists every model and test with its status and timing.
            archiveArtifacts artifacts: 'dbt/target/run_results.json', allowEmptyArchive: true
        }
        success {
            echo "All checks passed. Built image hdb-airflow:${env.BUILD_NUMBER}."
        }
        failure {
            echo 'A stage failed. Open it in the stage view to see which step and why.'
        }
    }
}
