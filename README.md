# Retrieval enhanced legal assistant

### Note: This application is for demonstrational purposes only. It has been obfuscated to disallow usage out of the box.
## Table of Contents

1. [Overview](#overview)
2. [Application Structure](#application-structure)
3. [Data](#data)
4. [Retrieval Implementations](#retrieval-implementations)
5. [Augmentation Methods](#augmentation-methods)
6. [Evaluations](#evaluations)
7. [Demo](#demo)


## Overview

Since the discovery of large language models' remarkable ability to perform logical reasoning, there has been a debate among researchers and professionals regarding the potential for these models to replace tasks currently performed by human experts.

This repository hosts an implementation of a Polish AI legal assistant that uses legal data to perform QA over legal acts, utilizing an extensive dataset of questions and answers, keywords, precise citations, and legal acts. 

It incorporates unique chunking strategies for statutory law through tree representation, alongside state-of-the-art transformers for semantic search in the Polish language.

## Application Structure

![Application Structure](./assets/application_structure.png)

The core of the application is structured around an ETL (Extract, Transform, Load) pipeline that is responsible for handling legal data. It performs three primary functions: extracting data from various sources, transforming this data into a structured format, and loading it into the NoSQL database.

There are two main databases utilized within this system. The first is a NoSQL database, which is used for storing a comprehensive dataset that includes legal documents, keywords, and questions and answers (Q&A).

The second database is a specialized vector database, designed to store embeddings. Embeddings are high-dimensional vectors that represent the questions and legal documents in a numerical form, enabling efficient similarity searches.

At the heart of the interaction between these databases and the users is a FastAPI application. It facilitates the communication between the databases and the large language model. The model, serving as an AI agent, interprets user queries and fetches the required legal information from the databases.

Finally, there is the evaluation component, where the system's accuracy is analyzed based on the precise citations provided by specialized lawyers in response to user questions.
