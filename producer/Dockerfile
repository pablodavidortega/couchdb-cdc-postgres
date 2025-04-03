
FROM continuumio/miniconda3

WORKDIR /app

COPY environment.yml ./
RUN conda env create -f environment.yml

# Activate Conda environment
SHELL ["/bin/bash", "-c"]
RUN echo "conda activate env_name" >> ~/.bashrc
ENV PATH /opt/conda/envs/env_name/bin:$PATH

# Install required packages
RUN conda install -n env_name -y \
    python \
    requests \
    psycopg2 \
    kafka-python \
    && conda clean --all -f -y
