import matplotlib.pyplot as plt

class QueryVis:
    def __init__(self, columns, results):
        self.columns = columns
        self.results = results

    def _data_vectors(self, x, cols):
        x_index = self.columns.index(x)
        x_data = [d[x_index] for d in self.results]
        y_data = []
        for col in cols:
            y_index = self.columns.index(col)
            y_data.append([d[y_index] for d in self.results])

        return (x_data, y_data)

    def scatter(self, x=None, y=None, z=None):
        data = self._data_vectors(x, [y])
        plt.scatter(data[0], data[1][0])
        plt.xlabel(x)
        plt.ylabel(y)

        plt.show()
        return

    def line(self, x, cols=None, title=None):
        cols = (cols
                if (type(cols) is list) | (type(cols) is tuple)
                else self.columns)

        data = self._data_vectors(x,cols)

        for i,line in enumerate(data[1]):
            plt.plot(data[0], line, label=cols[i])

        plt.xlabel(x)
        plt.title(title)
        plt.show()

    def bar(self, title=None, cols=None):
        cols = (cols
                if (type(cols) is list) | (type(cols) is tuple)
                else self.columns)

        data_x = [
            tuple([row[self.columns.index(key)]
                   for key in cols
                   ]) for row in self.results
            ]

        for row in data_x:
            plt.bar(range(len(cols)), row)

        plt.xticks(range(len(cols)), cols, rotation=90)
        plt.xlabel('count')
        plt.title(title)
        plt.show()


class VisExplorer:
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
