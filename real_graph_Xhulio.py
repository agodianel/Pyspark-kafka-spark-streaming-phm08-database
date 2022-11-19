#read input file
from numpy import loadtxt
from reliability.Fitters import Fit_Weibull_2P,Fit_Weibull_3P,Weibull_Distribution
from reliability.Probability_plotting import Weibull_probability_plot
import matplotlib.pyplot as plt
from reliability.Probability_plotting import plot_points
from reliability.Other_functions import make_right_censored_data, histogram
indice=0

#--------------------------------------------------------------------------------
#               GRAFICO 1 e 2
#--------------------------------------------------------------------------------
# while(1==1):
#     #file = open('/home/hadoop/array.csv', 'rb')
#     file = open('/home/xhulio/progetti_kafka/Progetto_motori/array.csv', 'rb')
#     data = loadtxt(file,delimiter = ",")  
#     if(len(data)>0):
#       if(len(data)>indice):
#         indice=len(data)  
#         stream_wb=Fit_Weibull_2P(failures=data)
#         #stream_wb.distribution.SF(label='Fitted Distribution',color='steelblue')
#         #plot_points(failures=data,func='SF',label='failure data',color='red',alpha=0.7)
#         plt.ion()
#         plt.legend()
#         plt.pause(10)

#     else: print("wait")



#--------------------------------------------------------------------------------
#               GRAFICO 3
#--------------------------------------------------------------------------------

# while(1==1):
#     #file = open('/home/hadoop/array.csv', 'rb')
#     file = open('/home/xhulio/progetti_kafka/Progetto_motori/array.csv', 'rb')
#     data = loadtxt(file,delimiter = ",")  
#     if(len(data)>0):
#       if(len(data)>indice):
#         indice=len(data)  
#         stream_wb=Fit_Weibull_3P(failures=data,show_probability_plot=False,print_results=False)
#         stream_wb2=Fit_Weibull_2P(failures=data,show_probability_plot=False,print_results=False)
#         stream_wb2.distribution.PDF(label='Fit_Weibull_2P')  # plots to PDF of the fitted Weibull_3P
#         stream_wb.distribution.PDF(label='Fit_Weibull_3P', linestyle='--')  # plots to PDF of the fitted Weibull_3P
#         histogram(data,white_above=None)
#         plt.legend()
#         plt.ion()
#         plt.pause(10)

#     else: print("wait")

from reliability.Probability_plotting import PP_plot_semiparametric
from reliability.Fitters import Fit_Normal_2P
from reliability.Distributions import Weibull_Distribution
import matplotlib.pyplot as plt
from reliability.Distributions import Weibull_Distribution, Lognormal_Distribution, Exponential_Distribution
import matplotlib.pyplot as plt
import numpy as np


    #file = open('/home/hadoop/array.csv', 'rb')
file = open('/home/xhulio/progetti_kafka/Progetto_motori/array.csv', 'rb')
data = loadtxt(file,delimiter = ",")  
if(len(data)>0):
  if(len(data)>indice):
        indice=len(data)  
        stream_wb=Fit_Weibull_3P(failures=data,show_probability_plot=False,print_results=False)
        
        
        

infant_mortality = Weibull_Distribution(alpha=stream_wb.alpha,beta=stream_wb.beta).HF(xvals=data,label='infant mortality [Weibull]')
random_failures = Exponential_Distribution(Lambda=stream_wb.alpha).HF(xvals=data,label='random failures [Exponential]')
wear_out = Lognormal_Distribution(mu=stream_wb.alpha,sigma=stream_wb.beta).HF(xvals=data,label='wear out [Lognormal]')
combined = infant_mortality+random_failures+wear_out
plt.plot(data,combined,linestyle='--',label='Combined hazard rate')
plt.legend()
plt.title('Example of how multiple failure modes at different stages of\nlife can create a "Bathtub curve" for the total Hazard function')
plt.xlim(0,1000)
plt.ylim(bottom=0)
plt.show()