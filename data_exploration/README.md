# Data Exploration

## Getting Started

###Install
```
cd /dse203-group-project/data_exploration

pip install .
```

###Upgrade
```
pip install . --upgrade
```

###In Python
**product.config must be in the directory with the python code referencing dora**
```python
from dora.api import DataExplorer
d = DataExplorer()
response = d.products.ratingsDitribution()
```

