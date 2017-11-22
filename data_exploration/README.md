# Data Exploration

## Getting Started

### Install
```
git clone https://github.com/j-goldsmith/dse203-group-project.git
cd /dse203-group-project/data_exploration
pip install .
```

### Upgrade
```
pip install . --upgrade
```

### In Python
**product.config must be in the directory with the python code referencing dora**
```python
from dora.api import DataExplorer
d = DataExplorer()
response = d.products.ratingsDitribution()
```

