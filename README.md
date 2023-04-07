# cat-merge

Python library for merging individual source KGX files in the Monarch Initiative ingest pipeline. 

### Dependencies

- [Poetry](https://python-poetry.org/docs/)
- [Pandas](https://pandas.pydata.org/)


## Installation

cat-merge is available on PyPI, and can be installed with pip/pipx:
```bash
[pip|pipx] install cat-merge
```

Alternatively, clone the repository and install with poetry:
```bash
git clone https://github.com/monarch-initiative/cat-merge.git
cd cat-merge
poetry install
```

## Usage

### In CLI

cat-merge can be run from the command line:
```bash
$ cat-merge --help

Usage: cat-merge [OPTIONS]

  Merge knowledge graphs

Options:
  -n, --name TEXT        Name of KG to merge
  -i, --input_dir TEXT   Directory containing node and edge files
  -m, --mapping TEXT     Optional SSSOM mapping file(s)
  -o, --output_dir TEXT  Directory to output knowledge graph  [default: merged-output]
  -r, --qc_report        Boolean for whether to generate a qc report [default: True]
  --install-completion   Install completion for the current shell.
  --show-completion      Show completion for the current shell, to copy it or customize the installation.
  --help                 Show this message and exit.
```

Note that if you are using multiple mapping files, you must specify each with the `-m` flag. For example:
```bash
cat-merge -n example-kg -i example_data -m mapping1.tsv -m mapping2.tsv
```

### As a library

Import the merge tool:
```python
from cat_merge.merge import merge
```

You can either merge a list of node and edge files:
```python
merge(
    name='monarch-kg',
    nodes=['xenbase_gene_nodes.tsv','reactome_pathway_nodes.tsv','monarch_ontology_nodes.tsv'],
    edges=['xenbase_gene_to_phenotype_edges.tsv','monarch_ontology_edges.tsv']
)
```

Or merge an entire directory:
```python
merge(
    name='monarch-kg',
    input_dir='transform_output',
    output_dir='merged-output'
)
```