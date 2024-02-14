# Retrieval enhanced legal assistant

### Note: This application is for demonstrational purposes only. It has been obfuscated to disallow usage out of the box.
## Table of Contents

1. [Overview](#overview)
2. [Application Structure](#application-structure)
3. [Data Overview](#data-overview)
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


## Data Overview

The dataset designed to enhance large language models consists of three primary components: Q&A sessions, where users pose legal queries; legal documents, which provide the basis for answers; and keywords that highlight specific sections of these documents. The diagram below illustrates how these elements interact.

![Data Relations](./assets/data_relations.png)

As depicted, each query is associated with a set of keywords and corresponding legal documents. The metadata for each document includes related sections, akin to paragraphs within the document. Keywords serve as connectors, linking questions to relevant sections of these documents.

Leveraging the hierarchical structure of Polish legal documents, which can be depicted as a tree (where a depth-first search would recreate the entire document), I have developed a creative algorithm. The algorithm extracts subtrees from leaf nodes, generating semantically distinct and meaningful segments, as illustrated in the following diagram.

![Chunking](./assets/chunking.png)

This segmentation approach enables precise semantic searches, facilitates document reconstruction, and preserves inter-segment relationships, especially when the text length surpasses the limits imposed by RoBERTa encoder models. The dataset's structured interconnections enable the creation of advanced data retrieval strategies, further detailed in subsequent sections.

## Retrieval Implementations

This chapter describes two distinct approaches for extracting data. The initial approach relies solely on the user's original query as the basis for conducting similarity searches.

![Retrieval_1](./assets/retrieval_1.png)


As illustrated in the diagram, the user's original query is essential for conducting searches. There are two primary methods: a straightforward search, where the query is converted into a vector and a similarity search is performed within the "qdrant" act collection, and a two-step search. In the latter, the query first identifies the most closely related questions, and then, utilizing the associated acts and keywords identified by legal professionals in the questions metadata, it refines the search to those specific acts and keywords. The most similar vectors are identified, and the documents are reconstructed hierarchically based on their node IDs, yielding the search results.

The second approach involves multiple uses of a large language model (LLM) within the retrieval process.

![Retrieval_2](./assets/retrieval_2.png)

As depicted, incorporating LLMs as a reasoning mechanism enables more complex and refined search capabilities. Initially, the LLM can rephrase the original query to enhance search precision, given its expertise in specific domains. Furthermore, the LLM can generate multiple queries from the original, treating each as a distinct search to gather a broader range of results. These results are then merged and ranked using a technique known as reciprocal rank fusion. This method is advantageous as it explores various semantic contexts. The LLM can also refine the search by filtering through the metadata obtained from similar queries, selecting relevant keywords and acts while excluding those less likely to contain relevant legal information. Lastly, it can expand the query further, generating unique queries for each relevant act based on the metadata, enhancing the search's effectiveness.

There are many trade-offs to be considered, the time for the user to receive the answer, compute and possible hallucinations. In the next chapters I will discuss augmentation methods used during the experiments as well as some evaluations.