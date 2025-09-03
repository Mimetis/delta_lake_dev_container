# [Choice] Python version (use -bookworm or -bullseye variants on local arm64/Apple Silicon): 3, 3.13, 3.12, 3.11, 3.10, 3.9, 3-bookworm, 3.13-bookworm, 3.12-bookworm, 3.11-bookworm, 3.10-bookworm, 3.9-bookworm, 3-bullseye, 3.13-bullseye, 3.12-bullseye, 3.11-bullseye, 3.10-bullseye, 3.9-bullseye, 3-buster, 3.12-buster, 3.11-buster, 3.10-buster, 3.9-buster
FROM spark:4.0.0-scala2.13-java21-ubuntu

USER root

# Install Python and pip
RUN set -ex; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
	libbluetooth-dev tk-dev uuid-dev \	
	python3 python3-pip; \
    rm -rf /var/lib/apt/lists/*


# Temporary: Upgrade python packages due to https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2022-40897 and https://github.com/advisories/GHSA-2mqj-m65w-jghx
# They are installed by the base image (python) which does not have the patch.
RUN python3 -m pip install --upgrade \
    setuptools==75.6.0 \
    gitpython==3.1.41

#symlink
RUN ln -s /usr/bin/python3 /usr/bin/python

# Install specific versions of PySpark and Delta Lake
RUN pip install --upgrade pip
RUN pip install pyspark==4.0.0 delta-spark==4.0.0  
RUN pip install deltalake==1.0.2 jupyterlab==4.4.4 pandas==2.3.0 roapi==0.12.6
RUN pip install pyarrow
RUN pip install --upgrade --force-reinstall ipykernel ipython jupyterlab-widgets jupyterlab-server

# Download the Delta Lake JARs
RUN wget https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/4.0.0/delta-spark_2.13-4.0.0.jar
RUN wget https://repo1.maven.org/maven2/io/delta/delta-storage/4.0.0/delta-storage-4.0.0.jar

# access to adls gen 2
RUN wget https://repo1.maven.org/maven2/com/azure/azure-storage-blob/12.31.2/azure-storage-blob-12.31.2.jar
RUN wget https://repo1.maven.org/maven2/com/azure/azure-identity/1.17.0/azure-identity-1.17.0.jar
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.4.1/hadoop-azure-3.4.1.jar
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure-datalake/3.4.1/hadoop-azure-datalake-3.4.1.jar
# Copy the JARs to Spark's jars directory
RUN cp delta-spark_2.13-4.0.0.jar delta-storage-4.0.0.jar azure-storage-blob-12.31.2.jar hadoop-azure-3.4.1.jar hadoop-azure-datalake-3.4.1.jar azure-identity-1.17.0.jar $SPARK_HOME/jars/

# [Optional] If your pip requirements rarely change, uncomment this section to add them to the image.
# COPY requirements.txt /tmp/pip-tmp/
# RUN pip3 --disable-pip-version-check --no-cache-dir install -r /tmp/pip-tmp/requirements.txt \
#    && rm -rf /tmp/pip-tmp

# [Optional] Uncomment this section to install additional OS packages.
# RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
#     && apt-get -y install --no-install-recommends <your-package-list-here>

# [Optional] Uncomment this line to install global node packages.
# RUN su vscode -c "source /usr/local/share/nvm/nvm.sh && npm install -g <your-package-here>" 2>&1