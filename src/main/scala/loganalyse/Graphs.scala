package loganalyse

import java.awt.Font
import java.awt.Color
import javax.swing.JFrame
import javax.swing.WindowConstants
import org.jfree.chart.ChartFactory
import org.jfree.chart.ChartPanel
import org.jfree.chart.JFreeChart
import org.jfree.chart.plot.PiePlot
import org.jfree.chart.axis.NumberAxis
import org.jfree.chart.plot.PlotOrientation
import org.jfree.chart.plot.XYPlot
import org.jfree.chart.plot.CategoryPlot
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer
import org.jfree.data.general.DefaultPieDataset
import org.jfree.data.general.PieDataset
import org.jfree.data.category.DefaultCategoryDataset
import org.jfree.data.xy.XYDataset
import org.jfree.data.xy.XYSeries
import org.jfree.data.xy.XYSeriesCollection
import org.jfree.ui.ApplicationFrame


object Graphs {
  
  def createPieChart(data:List[(Int,Int)]):JFrame={
    
    val dataset:DefaultPieDataset= new DefaultPieDataset()
    data.foreach(X=>dataset.setValue(X._1,X._2))
    val chart:JFreeChart = ChartFactory.createPieChart(
            "Response Code Pir Chart",  // chart title
            dataset,             // data
            true,               // include legend
            true,
            false
        )
    val plot:PiePlot = chart.getPlot().asInstanceOf[PiePlot]
        plot.setLabelFont(new Font("SansSerif", Font.PLAIN, 12))
        plot.setNoDataMessage("No data available")
        plot.setCircular(false)
        plot.setLabelGap(0.02)
        
    val frame:JFrame = new JFrame("Logfile-Analyse: Relative Häufigkeiten der Response Codes") 
    frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE)
    val chartPanel: ChartPanel = new ChartPanel(chart)
    frame.setContentPane(chartPanel)
    frame.pack()
    frame.setVisible(true)
    frame
  }
  
  def createLineChart(data:List[(Int,Int)], chartTitle:String, label_x:String, label_y:String):JFrame={
    
    
    val series: XYSeries = new XYSeries("data")
    data.foreach(X=>series.add(X._1,X._2))
    val dataset:XYSeriesCollection  = new XYSeriesCollection()
    dataset.addSeries(series)
    val chart:JFreeChart = ChartFactory.createXYLineChart(
            chartTitle,      // chart title
            label_x,                      // x axis label
            label_y,                      // y axis label
            dataset,                  // data
            PlotOrientation.VERTICAL,
            true,                     // include legend
            true,                     // tooltips
            false                     // urls
        )
    
    val plot:XYPlot  = chart.getXYPlot() 
    plot.setBackgroundPaint(Color.lightGray)
    plot.setDomainGridlinePaint(Color.white)
    plot.setRangeGridlinePaint(Color.white)
        
    val frame = new JFrame("Logfile-Analyse: Anzahl der Requests Pro Tag")
    frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE)    
    val chartPanel: ChartPanel = new ChartPanel(chart)
    frame.setContentPane(chartPanel)
    frame.pack()
    frame.setVisible(true)
    frame
  }
  
  def createBarChart(data:List[(Int,String)],chartTitle:String, label_x:String, label_y:String):JFrame={
    
   val dataset = new DefaultCategoryDataset()
   data.foreach(X=> dataset.addValue(X._1, X._2, ""))
   val chart:JFreeChart = ChartFactory.createBarChart(
            chartTitle,         // chart title
            label_x,               // domain axis label
            label_y,                  // range axis label
            dataset,                  // data
            PlotOrientation.VERTICAL, // orientation
            true,                     // include legend
            true,                     // tooltips?
            false                     // URLs?
        );
    chart.setBackgroundPaint(Color.white);

    // get a reference to the plot for further customisation...
    val plot:CategoryPlot = chart.getCategoryPlot();
    plot.setBackgroundPaint(Color.lightGray);
    plot.setDomainGridlinePaint(Color.white);
    plot.setRangeGridlinePaint(Color.white);

    val frame = new JFrame("Logfile-Analyse: Anzahl der Requests Pro Tag")
    frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE)    
    val chartPanel: ChartPanel = new ChartPanel(chart)
    frame.setContentPane(chartPanel)
    frame.pack()
    frame.setVisible(true)
    frame
  }
}