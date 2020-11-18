
FROM python:3.7-stretch

COPY ./src/requirements.txt /tmp

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /tmp/requirements.txt


#Setting jupyter password to 'pricing123'
RUN mkdir /root/.jupyter
COPY ./jupyter_notebook_config.py /root/.jupyter

RUN jupyter contrib nbextension install --user
RUN jupyter nbextensions_configurator enable --user


#Installing java for databricks connect
RUN apt-get update \
	&& apt-get install -y software-properties-common\
	&& add-apt-repository -y ppa:webupd8team/java\
	&& apt-get install -y default-jdk

RUN mkdir /home/pricing
WORKDIR /home/pricing

ENV PRICING_API_HOST http://177.103.230.68:17000/
ENV PRICING_API_USER pricing_api_username
ENV PRICING_API_PASSWORD 7b4ps434fku4uxxehak5dr3qpe3k8472

COPY ./.databricks-connect /root

CMD jupyter notebook --port=8888 --no-browser --ip=0.0.0.0 --allow-root \
	& python src/pricing/dashboard/index.py >> /var/log/pricing.stdout.log 2>&1
