# Jenkins controller for this project's CI, with the tools the pipeline stages need baked in.
FROM jenkins/jenkins:lts-jdk17

USER root

# python3 for ruff + dbt, psql to load the sample data into the test database.
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-venv postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Same pinned dbt as the Airflow image and GitHub Actions, in its own virtualenv.
RUN python3 -m venv /opt/ci-venv \
    && /opt/ci-venv/bin/pip install --no-cache-dir "dbt-postgres==1.11.0" ruff

# Docker CLI (+ buildx) only. Builds run on the host's Docker engine through the mounted socket.
COPY --from=docker:29-cli /usr/local/bin/docker /usr/local/bin/docker
COPY --from=docker:29-cli /usr/local/libexec/docker/cli-plugins/docker-buildx /usr/local/libexec/docker/cli-plugins/docker-buildx

USER jenkins

# Plugins are installed at build time, so a fresh container needs no clicking through setup.
COPY plugins.txt /usr/share/jenkins/ref/plugins.txt
RUN jenkins-plugin-cli --plugin-file /usr/share/jenkins/ref/plugins.txt

# Configuration as code: skip the setup wizard and create the pipeline job from casc.yaml.
COPY casc.yaml /var/jenkins_conf/casc.yaml
ENV CASC_JENKINS_CONFIG=/var/jenkins_conf/casc.yaml
ENV JAVA_OPTS="-Djenkins.install.runSetupWizard=false"
