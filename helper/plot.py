from pyecharts.charts import Line, Bar
from pyecharts import options as opts


def createPlotLine(df, xAxisTitle, yAxisTitles, title=None):
    if not type(yAxisTitles) == list:
        print("yAxisTitles shoulf be an array!")

    line = Line()
    line.add_xaxis(df[xAxisTitle].tolist())
    for yTitle in yAxisTitles:
        line.add_yaxis(yTitle, df[yTitle].tolist())
    
    if not title:
        title = ""
    line.set_global_opts(
        tooltip_opts=opts.TooltipOpts(trigger='axis'),
        title_opts=opts.TitleOpts(title=title))

    return line