FROM python:3.9

LABEL maintainer="READ-NEED Core Maintainers <deeps@readneed.org>"

WORKDIR /code
COPY ./requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./ /code/

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80", "--reload"]