from typing import Optional, Tuple

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.figure import Figure
from pandas import DataFrame, Series


class Basecharts:
    "Defines the configurations for base charts"

    # step color palette for charts
    colors = [
        "#EF476F",
        "#FFD166",
        "#06D6A0",
        "#118AB2",
        "#0F9F2F",
        "#38a1db",
        "#ff7b54",
        "#f1a156",
        "#ff557f",
        "#4acfac",
        "#726a95",
        "#f08a5d",
        "#7966cc",
        "#fc8f79",
        "#adece9",
        "#49beb7",
        "#9e8194",
        "#5b4f70",
        "#d4a5a5",
        "#ff6e40",
        "#99ddcc",
        "#934782",
        "#9fd6e2",
        "#ffa62b",
    ]

    @classmethod
    def pie(
        cls,
        title: str,
        donut_shape: Optional[float],
        summary: Series,
        figsize: Tuple[int, int] = (10, 6),
        autopct: str = "%1.1f%%",
    ) -> Figure:
        """
        Generates a pie or donut chart and returns a matplotlib Figure.

        :param title: Title of the chart
        :param donut_shape: Determines the size of the inner circle to make the pie chart appear as a donut chart. If None, will produce a pie chart.
        :param summary: Pandas Series object containing the summary data to be plotted
        :param figsize: Tuple representing the figure size in inches. Default is (10, 6)
        :param autopct: String to define the numeric precision in string format. Default is "%1.1f%%"

        :returns: a matplotlib Figure object with the generated chart
        """
        figure = plt.figure(figsize=figsize)
        explode = [0.1 if i == 0 else 0 for i in range(len(summary))]
        plt.pie(
            summary,
            labels=summary.index,
            explode=explode,
            colors=cls.colors,
            autopct=autopct,
        )
        plt.gca().set_aspect("equal")

        if donut_shape:
            plt.gca().add_artist(plt.Circle((0, 0), donut_shape, color="white"))
        plt.title(title)
        return figure

    @classmethod
    def bar(
        cls,
        title: str,
        summary: Series,
        x_label: Optional[str] = None,
        y_label: Optional[str] = None,
        horizontal: bool = False,
        figsize: Tuple[int, int] = (10, 6),
    ) -> Figure:
        """
        This class method generates a bar chart using the summary data provided, either as a horizontal or vertical layout.

        :param title: The title of the chart.
        :param summary: The series object containing the data to be plotted.
        :param x_label: The label of the X-axis. Defaults to None.
        :param y_label: The label of the Y-axis. Defaults to None.
        :param horizontal: Defines the orientation of the chart. If true, the chart will be horizontal. If false, the chart will be vertical. Default is False.
        :param figsize: The size of the figure to be plotted. Defaults to (10, 6).

        :returns: A matplotlib figure containing the plotted bar chart.
        """

        figure = plt.figure(figsize=figsize)
        if horizontal:
            summary.plot(kind="barh", color=cls.colors)
            # Adding data labels to the bars
            for i, v in enumerate(summary):
                plt.text(v + 0.5, i, str(v), color="black", va="center", fontsize=10)
        else:
            summary.plot(kind="bar", color=cls.colors)
            # Adding data labels to the bars
            for i, v in enumerate(summary):  # Limit to top 10
                plt.text(
                    i,
                    v // 2,
                    str(v),
                    color="black",
                    ha="center",
                    va="center",
                    fontsize=10,
                )

        plt.title(title)
        if x_label:
            plt.xlabel(x_label, fontsize=12)
        if y_label:
            plt.ylabel(y_label, fontsize=12)

        return figure

    @classmethod
    def line(
        cls,
        title: str,
        summary: DataFrame,
        x: str,
        y: str,
        hue: Optional[str] = None,
        grid: bool = False,
        figsize: Tuple[int, int] = (10, 6),
    ) -> Figure:
        """
        This is a class method called "line" that creates a line plot using the Seaborn library.

        :param title: The title of the line plot.
        :param summary: The data to be plotted.
        :param x: The column name in the DataFrame to be used as the x-axis.
        :param y: The column name in the DataFrame to be used as the y-axis.
        :param hue: An optional column name in the DataFrame to be used for color differentiation.
        :param grid: Specifies whether to display grid lines.
        :param figsize: The size of the figure.

        The method uses the Seaborn library to create the line plot.
        If the "hue" parameter is provided, the plot will have different colors based on the values in the specified column.
        The "title" parameter is used to set the title of the plot. If "grid" is set to True, the plot will display grid lines.

        The method returns the created Figure object.
        """
        figure = plt.figure(figsize=figsize)
        if hue:
            sns.lineplot(data=summary, x=x, y=y, hue=hue)
        else:
            sns.lineplot(
                data=summary,
                x=x,
                y=y,
            )

        plt.title(title)
        if grid:
            plt.grid(True)
        return figure
