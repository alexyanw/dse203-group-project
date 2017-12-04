import matplotlib.pyplot as plt

class QueryViz:
    def __init__(self, columns, results):
        self.columns = columns
        self.results = results

    def bar(self, title=None, cols=None):
        cols = (cols
                if (type(cols) is list) | (type(cols) is tuple)
                else self.columns)

        data_x = [
            tuple([row[self.columns.index(key)]
                   for key in cols
                   ]) for row in self.results
            ]
        data_y = cols

        for row in data_x:
            plt.bar(range(len(cols)), row)

        # set the locations of the xticks

        # set the locations and labels of the xticks
        plt.xticks(range(len(cols)), cols, rotation=70)
        plt.xlabel('count')
        plt.title(title)
        plt.show()


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
