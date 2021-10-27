import datetime
import datetime as dt
from os.path import dirname, join
import numpy as np

import pandas as pd

import pyarrow as pa
import pyarrow.parquet as pq

from bokeh.io import curdoc
from bokeh.layouts import column, gridplot, row
from bokeh.models import ColumnDataSource, DataRange1d, Select, HoverTool, Panel, Tabs, LinearColorMapper, Range1d
from bokeh.models import NumeralTickFormatter, DatetimeTickFormatter, Title, Label, Paragraph, Div, CustomJSHover, BoxAnnotation
from bokeh.models import ColorBar
from bokeh.palettes import brewer, Spectral6
from bokeh.plotting import figure
from bokeh.embed import server_document
from bokeh.transform import factor_cmap

#################################################################################

height = int(1.15*533)
width = int(1.15*750)

start_date = "2013-01-01"
end_date = "2021-09-01"

end_range = dt.datetime(2022,1,1)

crl = ["darkblue","slategray","crimson"]

background = "#ffffff"

file = "./data"+ "/goals.parquet"

df = pq.read_table(file).to_pandas()

level = "All Phase One Products"

file = "./data"+ "/phaseone-tradedata-C-september.parquet"

timedf = pq.read_table(file).to_pandas()
#################################################################################

def make_source(df, goods_type):
    
    if chart_select.value == 'Overview 2020':
        values = ["2017 Values", "2020 Goals", "2020 Values"]
        year_value = "2020 Values"
        goals = "2020 Goals"
        
    if chart_select.value == 'Overview 2021':
        values = ["2017 Values", "2021 Goals", "2021 Values"]
        year_value = "2021 Values"
        goals = "2021 Goals"
    
    foo = df.loc[values,:]
    
    foo["position"] = foo.reset_index().index.values
    
    goal_met = foo[goods_type].loc[year_value] / foo[goods_type].loc[goals]

    goal_met = str(round(100*goal_met,1))
    
    foo["hover_label"] = (foo[goods_type]/1000000000).map('{:,.1f}'.format)
    
    source = ColumnDataSource(foo)
    
    return source, goal_met

##########################################################################

def make_trade_time(df, catagory):
    
    if catagory != "All Phase One Products":
    
        foo_df = df[df["high_catagory"] == catagory].copy()
                
    else:
        # Grab the catagory
        foo_df = df[df["high_catagory"] != "not in aggreement"].copy()
    
    foo_grp = foo_df.groupby(["time"])
    # need to groupby first, because within (what ever catagory) there are many
    # hs codes
    
    out_df = foo_grp.agg({"china_exports": "sum"})
    # then aggregate somehow, here we do this at the low_catagory level, could be hs2 or whatever
    
    idx = pd.date_range(start_date, end_date,freq='MS')
    
    out_df = out_df.reindex(idx, fill_value= np.nan)
    
    out_df.columns = [catagory]
    
    return out_df

##########################################################################
##########################################################################

def cum_trade(foo):
    
    outdf = pd.DataFrame([])
    
    outdf["cuml_trade_2017"] = foo[level_select.value].loc["2017"].cumsum()
        
    outdf.index = pd.date_range(start="2020-01-01", end="2020-12-01", freq = "MS")
    
    outdf["cuml_trade_2020"] = foo[level_select.value].loc["2020"].cumsum()
    
    return outdf

##########################################################################
##########################################################################

def make_bar_chart():

    source, goal_met = make_source(df, level_select.value)
    
    if chart_select.value == 'Overview 2021':
    
        title = " 2021 Overview of Purchase Commitments for " + level_select.value
        
    else:
        
        title = " 2020 Overview of Purchase Commitments for " + level_select.value

    p = figure(plot_height=height, plot_width = width, title = title,
           toolbar_location = 'below',
           tools = "reset")
    
    
    p.vbar(x = "position", top = level_select.value, width = 0.6, color = "colors", alpha = 0.75,
       hatch_pattern = " ",hatch_alpha = 0.10,
       source = source, legend_field=  "name")

##########################################################################
    TIMETOOLTIPS = """
    <div style="background-color:#F5F5F5; opacity: 0.95; border: 0px 0px 0px 0px">
        <div style = "text-align:left;">
            <span style="font-size: 13px; font-weight: bold">@name:</span>
        </div>
        <div style = "text-align:left;">
            <span style="font-size: 13px; font-weight: bold">$@hover_label Billion</span>
        </div>
    </div>
    """

    p.add_tools(HoverTool(tooltips = TIMETOOLTIPS))
##########################################################################

    #p.ygrid.grid_line_color = None
    p.xgrid.grid_line_color = None
    
    p.title.text_font_size = '13pt'
    p.xaxis.major_tick_line_color = None  # turn off x-axis major ticks
    p.xaxis.minor_tick_line_color = None  # turn off x-axis minor ticks
    p.xaxis.major_label_text_font_size = '0pt'  # turn off x-axis tick labels

    p.yaxis.formatter = NumeralTickFormatter(format="($0. a)")
    p.yaxis.minor_tick_line_color = None
    p.y_range.start = 0 
    


    p.y_range.end = 200000000000
    mytext = Label(x=605, y=370, text='''China's progress towards''', text_font_size="1.2em", text_font_style = "bold",
                      x_units='screen', y_units='screen')
    p.add_layout(mytext)
    mytext = Label(x=605, y=350, text='meeting commitments:', text_font_size="1.2em", text_font_style = "bold",
                      x_units='screen', y_units='screen')
    p.add_layout(mytext)
    mytext = Label(x=605, y=300, text= goal_met + '%', text_font_size="3em", text_font_style = "bold",
                      x_units='screen', y_units='screen')
    p.add_layout(mytext)

    
    p.border_fill_color = background    
    p.legend.orientation = "horizontal"
    p.legend.background_fill_color = background  
    p.legend.location = "top_left"
    p.legend.background_fill_alpha = 0.10
    p.legend.label_text_font_size = "1em"
    
    p.background_fill_color = background 
    p.background_fill_alpha = 0.75    
    
    p.toolbar.autohide = True
    
    p.outline_line_color = None
    p.sizing_mode= "scale_both"
    p.max_height = height
    p.max_width = width
    p.min_height = int(0.25*height)
    p.min_width = int(0.25*width)
    
    div0 = Div(text = """Offical Text from the <b>ECONOMIC AND TRADE AGREEMENT<b>""", width=555, background = background,
               style={"justify-content": "space-between", "display": "flex"} )
    div0.sizing_mode= "scale_both"
    
    div1 = Div(text="""<b>Chapter 6 (page 6-1), Article 6.2: Trade Opportunities.<b>""", width=555, background = background,
               style={"justify-content": "space-between", "display": "flex"} )
    
    div1.sizing_mode= "scale_both"
    
    if level_select.value == 'All Phase One Products':
        
        div2 = Div(text="""During the two-year period from January 1, 2020 through December 31, 2021, China shall
        ensure that purchases and imports into China from the United States of the manufactured goods,
        agricultural goods, energy products, ... exceed the corresponding
        2017 baseline amount by no less than $200 billion ($64 billion in calandar year 2020);""",
                   width=555,
                   background = background,
                   style={"justify-content": "space-between", "display": "flex"} )
        
        #div2.sizing_mode= "scale_both"
    
    if level_select.value == 'Manufactures':
        
        div2 = Div(text="""(a) For the category of manufactured goods identified in Annex 6.1, no less than $32.9
            billion above the corresponding 2017 baseline amount is purchased and imported
            into China from the United States in calendar year 2020, and no less than $44.8
            billion above the corresponding 2017 baseline amount is purchased and imported
            into China from the United States in calendar year 2021;""", width=555, background = background,
                   style={"justify-content": "space-between", "display": "flex"} )
        #div2.sizing_mode= "scale_both"
        
    if level_select.value  == 'Energy':
        
        div2 = Div(text="""(c) For the category of energy products identified in Annex 6.1, no less than $18.5
        billion above the corresponding 2017 baseline amount is purchased and imported into China 
        from the United States in calendar year 2020, and no less than $33.9
        billion above the corresponding 2017 baseline amount is purchased and imported
        into China from the United States in calendar year 2021;""", width=555, background = background,
                   style={"justify-content": "space-between", "display": "flex"} )
        #div2.sizing_mode= "scale_both" 
        
    if level_select.value  == 'Agriculture':
        
        div2 = Div(text="""(b) For the category of agricultural goods identified in Annex 6.1, no less than $12.5
        billion above the corresponding 2017 baseline amount is purchased and imported
        into China from the United States in calendar year 2020, and no less than $19.5
        billion above the corresponding 2017 baseline amount is purchased and imported
        into China from the United States in calendar year 2021;""", width=555, background = background,
                   style={"justify-content": "space-between", "display": "flex"} )
        #div2.sizing_mode= "scale_both" 
        
        
    p = column(p,div0,div1,div2,sizing_mode="scale_both", max_height = height, max_width = width,
              min_height = int(0.25*height), min_width = int(0.25*width))
    
    return p

##########################################################################
##########################################################################

def make_cum_purchase():
        
    title = "Cummulative US Exports to China of " + level_select.value
    
    foobar = make_trade_time(timedf,level_select.value)

    cuml = cum_trade(foobar)
    
    x = cuml.index
    y2017 = cuml["cuml_trade_2017"]
    y2020 = cuml["cuml_trade_2020"] 
    
    p = figure(x_axis_type="datetime", plot_height = height, plot_width=width, toolbar_location = 'below',
               tools = "box_zoom, reset, pan", title = title,
                  x_range = (dt.datetime(2020,1,1),dt.datetime(2020,12,15)) )

    p.line(x = x,
                  y = y2017, line_width=3.5, line_alpha=0.5, line_color = "crimson", line_dash = "dashed"
                  , legend_label= "2017")
        
    p.line(x = x,
                  y = y2020, line_width=3.5, line_alpha=0.75, line_color = "darkblue"
                  , legend_label= "2020")
                  
            
    singlesource2020 = ColumnDataSource({
                'xs': x.values,
                'ys': y2020.values,
                "dates": np.array(x),
                })
  
    c2020 = p.circle(x="xs", y="ys", size=35,
                    source = singlesource2020, color = "crimson",alpha=0.0)
    
    singlesource2017 = ColumnDataSource({
                'xs': x.values,
                'ys': y2017.values,
                "dates": np.array(pd.date_range(start="2017-01-01", end="2017-12-01", freq = "MS")),
                })
    
    c2017 = p.circle(x="xs", y="ys", size=35,
                    source = singlesource2017, color = "darkblue",alpha=0.0)

    TIMETOOLTIPS = """
            <div style="background-color:#F5F5F5; opacity: 0.95; border: 15px 15px 15px 15px;">
             <div style = "text-align:left;">"""
    
    TIMETOOLTIPS = TIMETOOLTIPS + """
            <span style="font-size: 13px; font-weight: bold"> @dates{%b %Y}:  $data_y{$0.0a}</span>   
            </div>
            </div>
            """
        
    p.add_tools(HoverTool(tooltips = TIMETOOLTIPS,  line_policy='nearest', formatters={'@dates': 'datetime'}, renderers = [c2017,c2020]))
        
    p.legend.title = 'Cumulative Purchases'
    p.legend.location = "top_left"
    p.legend.title_text_font_style = "bold"
    p.xaxis.formatter = DatetimeTickFormatter(months = ['%B'])

    p.title.text_font_size = '13pt'
    p.background_fill_color = background 
    p.background_fill_alpha = 0.75
    p.border_fill_color = background 
    
    tradewar_box = BoxAnnotation(left=dt.datetime(2018,7,1), right=dt.datetime(2019,10,11), fill_color='red', fill_alpha=0.1)
    p.add_layout(tradewar_box)
    
    tradewar_box = BoxAnnotation(left=dt.datetime(2020,1,1), right=dt.datetime(2021,12,31), fill_color='blue', fill_alpha=0.1)
    p.add_layout(tradewar_box)
    
    #p.yaxis.axis_label = 
    p.yaxis.axis_label_text_font_style = 'bold'
    p.yaxis.axis_label_text_font_size = "13px"

    p.yaxis.minor_tick_line_color = None
    
    p.yaxis.formatter = NumeralTickFormatter(format="($0. a)")
    
    div0 = Div(text = """Each line displays the cumulative purchases through the calendar month
    of the selected product category for the years 2017, 2020, and 2021.
    """, width=555, background = background )
    
    p.outline_line_color = None
    p.sizing_mode = "scale_both"
    div0.sizing_mode= "scale_both"
    
    p.toolbar.active_drag = None
    
    #p.WheelZoomTool.maintain_focus = False
    #print(p.y_range.end)
    p = column(p,div0, sizing_mode = "scale_both", max_height = height, max_width = width,
              min_height = int(0.25*height), min_width = int(0.25*width))
  
    return p


##########################################################################
##########################################################################

def make_time_by_product():
    
    title = "US Exports to China of " + level_select.value
    
    foobar = make_trade_time(timedf,level_select.value)
    
    start_range = dt.datetime(2017,7,1)
    #end_range = dt.datetime(2021,8,1)
        
    numlines=len(foobar.columns)
    
    multi_line_source = ColumnDataSource({
            'xs': [foobar.index.values]*numlines,
            'ys': [foobar[name].values for name in foobar.columns],
            'label': [name for name in foobar.columns],
            'line_color': ["crimson"] })
        
    p = figure(plot_height=height, plot_width = width, x_axis_type="datetime",toolbar_location = 'below',
           tools = "box_zoom, reset, pan, xwheel_zoom", title = title, x_range = (start_range,end_range) ) 

    p.multi_line(xs= "xs",
                ys= "ys",
                line_width=3.5, line_alpha=0.5, line_color = "line_color",
                 hover_line_alpha=0.75, hover_line_width = 5,
                hover_line_color= "crimson", source = multi_line_source)
    

    TIMETOOLTIPS = """
            <div style="background-color:#F5F5F5; opacity: 0.95; border: 5px 5px 5px 5px;">
            <div style = "text-align:left;">
            <span style="font-size: 13px; font-weight: bold"> @label
             </span>
             </div>
             <div style = "text-align:left;">"""
    
    
    TIMETOOLTIPS = TIMETOOLTIPS + """
            <span style="font-size: 13px; font-weight: bold"> $data_x{%b %Y}:  $data_y{$0.0a}</span>   
            </div>
            </div>
            """
    
    p.add_tools(HoverTool(tooltips = TIMETOOLTIPS,  line_policy='nearest', formatters={'$data_x': 'datetime'}))
    p.title.text_font_size = '13pt'
    p.background_fill_color = background 
    p.background_fill_alpha = 0.75
    p.border_fill_color = background 
    
    tradewar_box = BoxAnnotation(left=dt.datetime(2018,7,1), right=dt.datetime(2019,10,11), fill_color='red', fill_alpha=0.1)
    p.add_layout(tradewar_box)
    
    tradewar_box = BoxAnnotation(left=dt.datetime(2020,1,1), right=dt.datetime(2021,12,31), fill_color='blue', fill_alpha=0.1)
    p.add_layout(tradewar_box)
    
    #p.yaxis.axis_label = 
    p.yaxis.axis_label_text_font_style = 'bold'
    p.yaxis.axis_label_text_font_size = "13px"

    p.yaxis.minor_tick_line_color = None
    
    p.yaxis.formatter = NumeralTickFormatter(format="($0. a)")
    
    div0 = Div(text = """Red marks the period of Section 301 tariffs and retaliation. Blue is period of agreement.
    """, width=555, background = background )
    
    p.outline_line_color = None
    p.sizing_mode = "scale_both"
    div0.sizing_mode= "scale_both"
    
    p.toolbar.active_drag = None
    
    #p.WheelZoomTool.maintain_focus = False
    #print(p.y_range.end)
    p = column(p,div0, sizing_mode = "scale_both", max_height = height, max_width = width,
              min_height = int(0.25*height), min_width = int(0.25*width))
  
    return p

def makechart():
    
    if chart_select.value == 'Overview 2021':
        p = make_bar_chart()
        
    if chart_select.value == 'Overview 2020':
        p = make_bar_chart()
     
    if chart_select.value == "US Exports by Time":
        p = make_time_by_product()
        
    if chart_select.value == "Cumulative Purchases":
        p = make_cum_purchase()
        
    return p
        

##########################################################################

def update_plot(attrname, old, new):
    layout.children[0] = makechart()
    
# This part is still not clear to me. but it tells it what to update and where to put it
# so it updates the layout and [0] is the first option (see below there is a row with the
# first entry the plot, then the controls)

chart_select = Select(value='Overview 2021', title='Chart Type', options=['Overview 2021', 'Overview 2020', 'US Exports by Time', "Cumulative Purchases"], width=300)
# This is the key thing that creates teh selection object

chart_select.on_change('value', update_plot)

level_select = Select(value=level, title='Broad Product Category', options=['All Phase One Products', 'Manufactures', 'Agriculture', 'Energy'])

level_select.on_change('value', update_plot)

controls = column(chart_select,level_select)

layout = row(makechart(), controls)

curdoc().add_root(layout)
curdoc().title = "us-china-overview"