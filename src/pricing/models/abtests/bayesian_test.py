import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import os
import math
import random
import plotly.figure_factory as ff
from pprint import pprint
import scipy
import plotly.express as px
from pyspark.sql import SparkSession
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.sql.types import StringType
from unidecode import unidecode
py_round = round
from pyspark.sql.functions import *
pd.set_option('display.max_columns', None)
from datetime import *


class BayesianABTest:

  def __init__(self, test_type, alternatives, hist_data=None, n_samples = 10000):
    """
    Args:
        test_type (string): one of the implemented test types ('conversion','aov','arpu')
        alternatives (list): list of strings representing the alternatives
        hist_data (dict) : it is used to estimate priors based on historical
               pattern of conversion and aov. Should be a dict
               with format: {
              'conversion':{
                'mean': --
                'std': --
              }
              'aov':{
                'mean': --
              } 'std': --
            }

        n_samples (int, optional): # of samples for the MC simulations

    """
    implemented_test_types = ('conversion','aov','arpu')

    if test_type not in implemented_test_types:
      raise ValueError("ABTest for type " + str(type) + " not available yet.")

    if len(alternatives)!=2:
      raise NotImplemented("ABTest implemented for just 2 alternatives")

    self.test_type = test_type
    self.alternatives = alternatives
    self.n_samples = n_samples
    self.priors = {alternative:self._compute_prior(hist_data) for alternative in alternatives}
    self.posteriors = {alternative:{} for alternative in alternatives}
    self.has_data = False #flag to indicate if posterios were computed

    print("Initiating ABTests with priors:")
    pprint(self.priors)

  def feed_alternative_data(self,alternative,n_visits= None,n_paids = None, revenue = None):
    """
    Feeds alternative with data, to compute its posterior pdf. According to the type of test
    the right kind of data should be inputted

    Args:
        alternative (str): alternative name
        n_visits (float): #customers in the test (required for test_types 'arpu','conversion')
        n_paids (float): #of paid customers in the test (required for test_types 'arpu','conversion','aov')
        revenue (float): total revenue (required for test_types 'arpu','aov')
    """
    assert alternative in self.priors.keys(), "Alternative " + alternative + " not in ABTest"

    for parameter,sub_parameters in self.priors[alternative].items():
      self.posteriors[alternative].update(
        self._compute_posterior(parameter,sub_parameters,n_visits,n_paids,revenue)
      )

    self.has_data = True
    for alternative,parameters in self.posteriors.items():
      self.has_data = self.has_data & (parameters!={})

  def compute_stats(self,alternative_A,alternative_B):
    """
    Computes all relevant test statistics based on the posterior distribution of the alternatives

    Args:
        alternative_A (str): name of the alternative treated as control (A)
        alternative_B (str): name of the alternative treated as test (B)

    Returns:
        dict: Statistics are kept in a dict with keys:
        'prob2beat': Prob(alternative_B > alternative_A)
        'lift': expected value of the lift (absolute) in alternative_B vs alternative_A
        'perc_lift': expected value of the lift (percentage) in alternative_B vs alternative_A
        'expLift': expected value of the lift (absolute) choosing alternative_B given that alternative_B is better than A,
        'expPercLift': as 'expLift' but in percentage
        'expLoss': expected value of the loss (absolute) choosing alternative_B given that alternative_B is worse than A,
        'expPercLoss': as 'expLoss' but in percentage
        'arpu_mean': mean of the arpu for each alternative
    """
    assert (self.posteriors[alternative_A]!={}) & (self.posteriors[alternative_B]!={}),\
         "No data fed to all alternatives of the test"

    if self.test_type == 'conversion':
      raise NotImplemented()
    elif self.test_type == 'aov':
      raise NotImplemented()
    elif self.test_type == 'arpu':

      #samples for MC
      smp = {
        'lambdaA': None,
        'lambdaB': None,
        'thetaA' : None,
        'thetaB' : None,
        'arpuA': None,
        'arpuB': None
      }
      for alternative,_lambda,_theta,_arpu in zip([alternative_A,alternative_B],['lambdaA','lambdaB'],['thetaA','thetaB'],['arpuA','arpuB']):
        # print(alternative,_lambda,_theta)

        smp[_lambda] = self._sample_dist('lambda',self.posteriors[alternative]['lambda'])
        smp[_theta] = self._sample_dist('theta',self.posteriors[alternative]['theta'])
        smp[_arpu] = smp[_lambda]/smp[_theta]

      # Modeling ARPU
      # lift = smp['lambdaB']/smp['thetaB'] - smp['lambdaA']/smp['thetaA']
      lift = smp['arpuB'] - smp['arpuA']
      perc_lift = smp['arpuB']/smp['arpuA'] - 1
      return {
        'prob2beat': (smp['lambdaB']/smp['thetaB'] > smp['lambdaA']/smp['thetaA']).sum()/self.n_samples,
        'lift': lift.mean(),
        'perc_lift':perc_lift.mean(),
        'expLift': py_round((lift*(lift>0)).mean(),2),
        'expPercLift': (perc_lift*(perc_lift>0)).mean(),
        'expLoss': py_round((-lift*(-lift>0)).mean(),2),
        'expPercLoss': (-perc_lift*(-perc_lift>0)).mean(),
        'arpu_mean': {
          alternative_A: (smp['lambdaA']/smp['thetaA']).mean(),
          alternative_B: (smp['lambdaB']/smp['thetaB']).mean(),
        }
      }

  def plot_cumulative_results(self,df_cum_results,ax = None,plotly=False):

    prob2beat = {alternative:[] for alternative in self.alternatives}
    expLoss = {alternative:[] for alternative in self.alternatives}
    # day_list = df_cum_results['created_day'].dt.strftime('%Y-%m-%d').unique().tolist()
    day_list = df_cum_results['created_day'].unique().tolist()

    for day in day_list:
      day_results = df_cum_results[df_cum_results['created_day']==day]
      for alternative in self.alternatives:
        self.feed_alternative_data(
          alternative,
          n_visits = day_results[day_results['alternative']==alternative]['n_visits'].values[0],
          n_paids = day_results[day_results['alternative']==alternative]['n_paids'].values[0],
          revenue = day_results[day_results['alternative']==alternative]['revenue'].values[0]
        )

      for alternative_A, alternative_B in zip(self.alternatives, self.alternatives[::-1]):
        stats = self.compute_stats(alternative_A,alternative_B)
        prob2beat[alternative_B].append(stats['prob2beat'])
    #         expLoss[alternative_B].append(stats['expLoss'])
        expLoss[alternative_B].append(stats['expPercLoss'])

    if plotly:
      return self._plot_cummulative_results_plotly(day_list,prob2beat,expLoss)
    else:

      if ax is None:
        returnAx = True
        fig,ax = plt.subplots(2,1,figsize=(8,10))
      else:
        returnAx = False

      for alternative in self.alternatives:
        ax[0].plot(day_list,prob2beat[alternative],label = alternative)
        ax[1].plot(day_list,expLoss[alternative],label = alternative)

      ax[0].legend()
      ax[0].set_ylim(0,1)
      ax[0].set_title('Probability to beat alternative')

  def _plot_cummulative_results_plotly(self,day_list,prob2beat,expLoss):

    def plotly_format(metric_dict,day_list):
      df = pd.DataFrame(metric_dict)
      df['Data'] = day_list
      return df.melt(id_vars = 'Data', value_vars=list(metric_dict.keys()))

    fig_prob2beat = px.line(plotly_format(prob2beat,day_list),x="Data", y='value',color = 'variable')
    fig_expLoss = px.line(plotly_format(expLoss,day_list), x="Data", y='value',color = 'variable')

    return fig_prob2beat,fig_expLoss

  def plot_results(self,ax=None,plotly=False,):
    assert self.has_data,"No data fed to all alternatives of the test"

    if self.test_type == 'conversion':
      raise NotImplemented()
    elif self.test_type == 'aov':
      raise NotImplemented()
    elif self.test_type == 'arpu':

      data = []
      labels = []
      for alternative in self.priors.keys():
        lambda_sample = self._sample_dist('lambda',self.posteriors[alternative]['lambda'])
        theta_sample = self._sample_dist('theta',self.posteriors[alternative]['theta'])

        arpu_sample = lambda_sample*(1/theta_sample)
        data.append(arpu_sample)
        labels.append(alternative)

      if plotly:
        return self._plot_results_plotly(data,labels)
      else:
        if ax is None:
          fig,ax = plt.subplots(1,1,figsize=(15,10))

        for i in range(len(data)):
          sns.distplot(data[i],hist=False,ax=ax,label=labels[i])

        ax.set_title("Distribuição do ARPU para ambas alternatives", fontsize = 14)

  def _plot_results_plotly(self,data,labels):
    fig = ff.create_distplot(data,labels,show_rug = False,show_hist =False)
    return fig

  def plot_parameter_distributions(self):
    """Summary

    Raises:
        NotImplemented: Description
    """
    assert self.has_data,"No data fed to all alternatives of the test"

    def _plot_distribution(parameter,sub_parameters,ax,label = None):
      """Summary

      Args:
          parameter (TYPE): Description
          sub_parameters (TYPE): Description
          ax (TYPE): Description
          label (None, optional): Description
      """
      samples = self._sample_dist(parameter,sub_parameters)
      sns.distplot(samples,hist=False,ax= ax, label = label)

    if self.test_type == 'conversion':
      raise NotImplemented()
    elif self.test_type == 'aov':
      raise NotImplemented()
    elif self.test_type == 'arpu':

      #Parameter plots
      fig_param,ax_param = plt.subplots(3,2,figsize=(15,10))

      #Titles
      ax_param[0][0].set_title("Parameter Lambda")
      ax_param[0][1].set_title("Parameter Theta")

      #Priors x Posteriors
      for alternative_idx,alternative in enumerate(self.priors.keys()):

        #Lambda
        ax_param[alternative_idx][0].set_ylabel("Alternative " + str(alternative))
        _plot_distribution('lambda',self.priors[alternative]['lambda'],ax_param[alternative_idx][0],label ='Prior')
        _plot_distribution('lambda',self.posteriors[alternative]['lambda'],ax_param[alternative_idx][0],label ='Posterior')
        _plot_distribution('lambda',self.posteriors[alternative]['lambda'],ax_param[-1][0],label =alternative)

        #Theta
        _plot_distribution('theta',self.priors[alternative]['theta'],ax_param[alternative_idx][1],label ='Prior')
        _plot_distribution('theta',self.posteriors[alternative]['theta'],ax_param[alternative_idx][1],label ='Posterior')
        _plot_distribution('theta',self.posteriors[alternative]['theta'],ax_param[-1][1],label = alternative)

      ax_param[-1][0].set_ylabel("Comparison of alternative's posteriors")


  def _compute_prior(self,hist_data=None):
    """
    Computes the prior distribution based on historical data

    Args:
        hist_data (dict): see @__init__

    Returns:
        dict: dict of type:
        {<parameter>:{
        <sub_parameter_1>: <value>,
        <sub_parameter_2>: <value>
        }}
    """
    def compute_prior_conversion(hist_data):
      """Summary

      Args:
          hist_data (TYPE): Description

      Returns:
          TYPE: Description
      """
      #Conversion is modelled as ~Bernoulli(lambda) with prior lambda ~Beta(alpha,beta)

      #non-informative prior
      alpha = 1
      beta = 1

      if hist_data is not None:
        if 'conversion' in hist_data.keys():
          print("Warning!! Using std as .1 @compute_prior_conversion")
          alpha,beta = self._estimate_beta_dist(
            hist_data['conversion']['mean'],
            .1
          )

      return {'lambda':{'alpha':alpha,'beta':beta}}

    def compute_prior_aov(hist_data):
      """Summary

      Args:
          hist_data (TYPE): Description

      Returns:
          TYPE: Description
      """
      #AOV (== ticket medio) is modelled as ~Exp(theta) with prior theta ~Gamma(k,omega)

      #non-informative prior
      k = 1
      omega = 1

      if hist_data is not None:
        if 'aov' in hist_data.keys():
          print("Warning!! Using std as .1 @ compute_prior_aov")
          k,omega = self._estimate_gamma_dist(
            1/hist_data['aov']['mean'],
            #1 #std of gamma(1,1)
            3e-03
          )

      return {'theta':{'k':k,'omega':omega}}

    def compute_prior_arpu(hist_data):
      """Summary

      Args:
          hist_data (TYPE): Description

      Returns:
          TYPE: Description
      """
      #ARPU is modelled as ~Bernoulli(lambda)*Exp(theta) with priors as in 'aov' and 'conversion'
      prior = {}
      prior.update(compute_prior_conversion(hist_data))
      prior.update(compute_prior_aov(hist_data))
      return prior

    func_map = {
      'conversion':compute_prior_conversion,
      'aov':compute_prior_aov,
      'arpu':compute_prior_arpu
    }

    return func_map[self.test_type](hist_data)

  def _compute_posterior(self,parameter_name,sub_parameters,n_visits = None,n_paids = None, revenue = None):
    """
    Computes the posterior distribution for a parameter passed as input given data

    Args:
        parameter_name (str): parameter name ('lambda' or 'theta')
        sub_parameters (dict): subparameters of the paramater (eg 'alpha','beta' for parameter 'lambda')
        n_visits (None, optional): see @feed_alternative_data
        n_paids (None, optional): see @feed_alternative_data
        revenue (None, optional): see @feed_alternative_data

    Returns:
        dict:  dict of type {<parameter>:{
        <sub_parameter_1>: <value>,
        <sub_parameter_2>: <value>,
        'mean': -- ,
        'std': --
        }}
      where 'mean'/'std' are the moments of posterior

    """
    if parameter_name == 'lambda':
      assert (n_visits!=None)&(n_paids!=None), "n_visits and n_paids required @compute_posterior with parameter == lambda"

      posterior = {parameter_name:{
        'alpha': sub_parameters['alpha'] + n_paids,
        'beta':  sub_parameters['beta'] + n_visits - n_paids
      }}
      posterior[parameter_name].update({
        'mean': scipy.stats.beta.mean(a=posterior[parameter_name]['alpha'],b=posterior[parameter_name]['beta']),
        'std': scipy.stats.beta.std(a=posterior[parameter_name]['alpha'],b=posterior[parameter_name]['beta'])
      })


    elif parameter_name =='theta':
      assert (n_paids!=None)&(revenue!=None), "n_paids and revenue required @compute_posterior with parameter == theta"

      posterior =  {parameter_name:{
        'k':sub_parameters['k'] + n_paids,
        'omega': sub_parameters['omega']/(1 + sub_parameters['omega']*revenue)
      }}

      posterior[parameter_name].update({
        'mean': scipy.stats.gamma.mean(a = posterior[parameter_name]['k'], scale = posterior[parameter_name]['omega']),
        'std': scipy.stats.gamma.std(a = posterior[parameter_name]['k'], scale = posterior[parameter_name]['omega'])
      })

    else:
      raise ValueError("Not modelling parameter ", parameter_name, " @BayesianABTest::compute_posterior")

    return posterior

  def _sample_dist(self,parameter,sub_parameters):
    """
    Samples self.n_samples from the distribution of a given parameter passed as input,
    for MC simulations

    Args:
        parameter (str): parameter ('lambda','theta')
        sub_parameters (dict): subparameters of the paramater (eg 'alpha','beta' for parameter 'lambda')

    Returns:
        np.array: samples from the distribution
    """
    if parameter == 'lambda':
      return np.random.beta(
        a = sub_parameters['alpha'],
        b = sub_parameters['beta'],
        size = self.n_samples
      )
    elif parameter == 'theta':
      return np.random.gamma(
        shape = sub_parameters['k'],
        scale = sub_parameters['omega'],
        size = self.n_samples
      )
    else:
      raise ValueError("Parameter " + parameter  +  " not known")

  def _estimate_beta_dist(self,mean,std):
    """
    Returns the parameters of a Beta distribution with a given mean/std (inputted)

    Args:
        mean (float): mean of the desired Beta distribution
        std (float): std of the desired Beta distribution

    Returns:
        float: alpha,beta Beta subparameters
    """
    var = std*std
    alpha = ( (1-mean)/var -1/mean)*mean*mean
    beta = alpha*(1/mean -1)

    assert (alpha>0) & (beta>0), "No beta distribution with mean: "+ str(mean)  + " and variance: " + str(var)
    return alpha,beta

  def _estimate_gamma_dist(self,mean,std):
    """
    Returns the parameters of a Gamma distribution with a given mean/std (inputted)

    Args:
        mean (float): mean of the desired Gamma distribution
        std (float): std of the desired Gamma distribution

    Returns:
        float: k,omega -> Gamma subparameters
    """
    var = std*std
    k = mean*mean/var
    omega = var/mean

    assert (k>0) & (omega>0), "No gamma distribution with mean: "+ str(mean)  + " and variance: " + str(var)
    return k,omega
