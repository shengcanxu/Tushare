from pyecharts.charts import Line, Bar, Kline, Grid
from pyecharts import options as opts
import helper.getDataFromDB as DBLib


def createPlotLines(df, xAxisTitle, yAxisTitles, title=None):
    if not type(yAxisTitles) == list:
        print("yAxisTitles shoulf be an array!")

    line = Line()
    line.add_xaxis(df[xAxisTitle].tolist())
    for yTitle in yAxisTitles:
        line.add_yaxis(yTitle, df[yTitle].tolist())
    
    if not title:
        title = ""
    line.set_global_opts(
        tooltip_opts=opts.TooltipOpts(trigger='axis', axis_pointer_type='cross'),
        title_opts=opts.TitleOpts(title=title)
    )
    return line


def createPlotLine(df, xAxisTitle, yAxisTitle, title=None):
    line = Line()
    line.add_xaxis(df[xAxisTitle].tolist())
    line.add_yaxis(yAxisTitle, df[yAxisTitle].tolist())
    
    if not title:
        title = ""
    line.set_global_opts(
        tooltip_opts=opts.TooltipOpts(trigger='axis', axis_pointer_type='cross'),
        title_opts=opts.TitleOpts(title=title)
    )

    ptg75 = df[yAxisTitle].quantile(.75)
    ptg50 = df[yAxisTitle].quantile(.5)
    ptg25 = df[yAxisTitle].quantile(.25)
    line.set_series_opts(
        markline_opts=opts.MarkLineOpts(
            data=[
                opts.MarkLineItem(y=ptg75, name="75%"),
                opts.MarkLineItem(y=ptg50, name="50%"),
                opts.MarkLineItem(y=ptg25, name="25%")
            ]
        ),
    )

    return line


def createKlineOfData(ts_code, startDate=None, endDate=None):
    stockName = DBLib.getStockDataFromDB(ts_code).loc[0, 'name']
    index = int(ts_code[len(ts_code)-5 : len(ts_code)-3]) % 30
    stockData = DBLib.getDataFromDB(code=ts_code, tableName='daily%d' % index, startDate=startDate, endDate=endDate)

    def calculate_ma(day_count: int, data):
        result = []
        for i in range(len(data)):
            if i < day_count:
                result.append(None)
                continue
            sum_total = 0.0
            for j in range(day_count):
                sum_total += float(data[i - j][1])
            result.append(abs(float("%.3f" % (sum_total / day_count))))
        return result

    kline_data = [[i[0], i[1], i[2], i[3]] for i in stockData[['open', 'close', 'low', 'high']].to_numpy()]
    volumes = [i for i in stockData['vol'].to_numpy()]
    xaxis_data = [i for i in stockData['trade_date'].to_numpy()]

    kline = (
        Kline()
        .add_xaxis(xaxis_data=xaxis_data)
        .add_yaxis(
            series_name=stockName,
            y_axis=kline_data,
            itemstyle_opts=opts.ItemStyleOpts(color="#ec0000", color0="#00da3c"),
        )
        .set_global_opts(
            title_opts=opts.TitleOpts(title="股票日K线"),
            yaxis_opts=opts.AxisOpts(
                is_scale=True,
                splitarea_opts=opts.SplitAreaOpts(
                    is_show=True, areastyle_opts=opts.AreaStyleOpts(opacity=1)
                ),
            ),
            xaxis_opts=opts.AxisOpts(is_scale=True),
            datazoom_opts=[
                opts.DataZoomOpts(
                    type_="inside",
                    xaxis_index=[0, 1],
                    range_start=int((len(stockData)-60)/len(stockData)*100),
                    range_end=100,
                ),
                opts.DataZoomOpts(
                    type_="slider",
                    pos_top="85%",
                    xaxis_index=[0, 1],
                    range_start=int((len(stockData)-60)/len(stockData)*100),
                    range_end=100,
                ),
            ],
            tooltip_opts=opts.TooltipOpts(
                trigger="axis",
                axis_pointer_type="cross",
                background_color="rgba(245, 245, 245, 0.8)",
                border_width=1,
                border_color="#ccc",
                textstyle_opts=opts.TextStyleOpts(color="#000"),
            ),
            axispointer_opts=opts.AxisPointerOpts(
                is_show=True,
                link=[{"xAxisIndex": "all"}],
                label=opts.LabelOpts(background_color="#777"),
            ),
        )
    )

    line = (
        Line()
        .add_xaxis(xaxis_data=xaxis_data)
        .add_yaxis(
            series_name="MA5",
            y_axis=calculate_ma(day_count=5, data=kline_data),
            is_smooth=True,
            is_hover_animation=False,
            is_symbol_show=False,
            linestyle_opts=opts.LineStyleOpts(width=2, opacity=0.5),
            label_opts=opts.LabelOpts(is_show=False),
        )
        .add_yaxis(
            series_name="MA10",
            y_axis=calculate_ma(day_count=10, data=kline_data),
            is_smooth=True,
            is_hover_animation=False,
            is_symbol_show=False,
            linestyle_opts=opts.LineStyleOpts(width=2, opacity=0.5),
            label_opts=opts.LabelOpts(is_show=False),
        )
        .add_yaxis(
            series_name="MA30",
            y_axis=calculate_ma(day_count=30, data=kline_data),
            is_smooth=True,
            is_hover_animation=False,
            is_symbol_show=False,
            linestyle_opts=opts.LineStyleOpts(width=2, opacity=0.5),
            label_opts=opts.LabelOpts(is_show=False),
        )
        .add_yaxis(
            series_name="MA60",
            y_axis=calculate_ma(day_count=60, data=kline_data),
            is_smooth=True,
            is_hover_animation=False,
            is_symbol_show=False,
            linestyle_opts=opts.LineStyleOpts(width=2, opacity=0.5),
            label_opts=opts.LabelOpts(is_show=False),
        )
    )
    # Kline And Line
    overlap_kline_line = kline.overlap(line)

    bar = (
        Bar()
        .add_xaxis(xaxis_data=xaxis_data)
        .add_yaxis(
            series_name="Volume",
            y_axis=volumes,
            xaxis_index=1,
            yaxis_index=1,
            label_opts=opts.LabelOpts(is_show=False),
        )
        .set_global_opts(
            legend_opts=opts.LegendOpts(is_show=False),
        )
    )

    # Grid Overlap + Bar
    grid_chart = (
        Grid(init_opts=opts.InitOpts(
                width="1000px",
                height="600px",
                animation_opts=opts.AnimationOpts(animation=False),
            )
        )
        .add(
            overlap_kline_line,
            grid_opts=opts.GridOpts(pos_left="10%", pos_right="8%", height="50%"),
        )
        .add(
            bar,
            grid_opts=opts.GridOpts(pos_left="10%", pos_right="8%", pos_top="63%", height="16%"),
        )
    ) 

    return grid_chart