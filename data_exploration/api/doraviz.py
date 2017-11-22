import matplotlib.pyplot as plt
from dora import DataExplorer

class VizExplorer:
    def _data_from_response(self, x, y, response):
        x_index = response.columns.index(x)
        y_index = response.columns.index(y)

        x_data = [d[x_index] for d in response.results]
        y_data = [d[y_index] for d in response.results]

        return (x_data, y_data)

    def line(self, x, y, response):
        data = self._data_from_response(x,y,response)

        plt.plot(data[0], data[1])
        plt.xlabel(x)
        plt.ylabel(y)

        plt.show()
        return


    def bar(self, x, y, response):
        data = self._data_from_response(x, y, response)

        plt.bar(data[0], data[1])
        plt.xlabel(x)
        plt.ylabel(y)

        plt.show()
        return

    def scatter(self, x, y, response):
        data = self._data_from_response(x, y, response)

        plt.scatter(data[0], data[1])
        plt.xlabel(x)
        plt.ylabel(y)

        plt.show()
        return


if __name__ == '__main__':
    explorer = DataExplorer()
    viz = VizExplorer()

    stats = explorer.orders.statsByProduct(order_by='numunits_sum')
    viz.bar('days_on_sale','numunits_sum', stats)
