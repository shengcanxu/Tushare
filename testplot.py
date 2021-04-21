from pyecharts import Bar
from pyecharts import Line
from pyecharts import Style
from pyecharts import Page
from pyecharts import Overlap

def create_charts():
    page = Page()
    x = ['{}年'.format(i)for i in range(1,12)]
    y = [3,5,3,5,3,4,5,3,5,2,4]
    y1=[1,2,3,4,5,6,7,8,9,10,11]
    style = Style(height=600,width=1400)
    bar = Bar('柱形图',**style.init_style,background_color=['pink'])
    line = Line()
    line.add('',x,y,effect_scale=8)
    line.add('',x,y1,effect_scale=10)
    bar.add('商家A',x,y,mark_line=['average'],mark_point=['min','max'])
    bar.add('商家B',x,y1,mark_line=['average'],mark_point=['min','max'],is_legend_show=True)
    overlap = Overlap(height=450,width=1200)
    overlap.add(bar)
    overlap.add(line)
    page.add(overlap)
    return page
    
create_charts().render('1.html')
