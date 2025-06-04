# SC3020: Database System Principles - Group 1

## Project 2: Understanding Cost Estimation IN RDBMS

```
 $$$$$$\  $$\   $$\ $$$$$$$\  $$$$$$$$\ $$\   $$\
$$  __$$\ $$ |  $$ |$$  __$$\ $$  _____|$$ |  $$ |
$$ /  $$ |$$ |  $$ |$$ |  $$ |$$ |      \$$\ $$  |
$$ |  $$ |$$ |  $$ |$$$$$$$  |$$$$$\     \$$$$  /
$$ |  $$ |$$ |  $$ |$$  ____/ $$  __|    $$  $$<
$$ $$\$$ |$$ |  $$ |$$ |      $$ |      $$  /\$$\
\$$$$$$ / \$$$$$$  |$$ |      $$$$$$$$\ $$ /  $$ |
 \___$$$\  \______/ \__|      \________|\__|  \__|
     \___|

```

**QUery Plan EXplainer**

QUPEX aims to explain the derived cost from PostgreSQL `EXPLAIN` feature. This involves understanding the cost model of PostgreSQL internal working (refer to: [costsize.c](https://github.com/postgres/postgres/blob/master/src/backend/optimizer/path/costsize.c)) and database catalog. The explanation that we provide for the cost derivation is emphasizing on how the formula used and how/why certain parameters used. In the case we are unable to estimate correctly, we also do provide more explantions for better clarity.

#### Team members:

- Clayton Fernalo
- Nema Aarushi
- Kristian Hadinata Achwan
- Lau Yong Jie

## Installation Guides

1. Install dependencies with the command:

```
pip install -r requirements.txt
```

2. Run the `project.py`

```
python project.py
```

## Technology Used

- Language: Python
- GUI Framework: [ttkbootstrap](https://ttkbootstrap.readthedocs.io/en/latest/)
- DB adaptor: [psycopg2](https://pypi.org/project/psycopg2/)
- Visualization: [graphviz](https://pypi.org/project/graphviz/)
