import matplotlib.pyplot as plt
from .api import DataExplorer

class VizExplorer:
    def _data_from_response(self, response, x, y, z):
        x_index = response.columns.index(x)
        y_index = response.columns.index(y)

        x_data = [d[x_index] for d in response.results]
        y_data = [d[y_index] for d in response.results]

        return (x_data, y_data)

    def line(self, query_response, x=None, y=None, z=None):
        data = self._data_from_response(query_response,x,y,z)

        plt.plot(data[0], data[1])
        plt.xlabel(x)
        plt.ylabel(y)

        plt.show()
        return


    def bar(self, query_response, x=None, y=None, z=None):
        data = self._data_from_response(query_response, x, y, z)

        plt.bar(data[0], data[1])
        plt.xlabel(x)
        plt.ylabel(y)

        plt.show()
        return

    def scatter(self, query_response, x=None, y=None, z=None):
        data = self._data_from_response(query_response, x, y, z)

        plt.scatter(data[0], data[1])
        plt.xlabel(x)
        plt.ylabel(y)

        plt.show()
        return


if __name__ == '__main__':
    explorer = DataExplorer()
    viz = VizExplorer()

    stats = explorer.orders.statsByProduct(order_by='numunits_sum')
    viz.bar(stats, 'days_on_sale','numunits_sum')
