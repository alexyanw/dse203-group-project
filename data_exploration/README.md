# Data Exploration

## Getting Started

### Install
```
git clone https://github.com/j-goldsmith/dse203-group-project.git
cd dse203-group-project/data_exploration
pip install .
```

### Upgrade
```
pip install . --upgrade
```
Run an upgrade after any changes made to /dora. Notebook kernels must be restarted afterwards. 

### Usage
**production.config must be in the directory with the python code referencing dora - working on fix for default**
```python
from dora.api import DataExplorer
d = DataExplorer()
response = d.products.ratingsDistribution()
```

