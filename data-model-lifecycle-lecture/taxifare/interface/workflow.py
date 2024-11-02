# workflow.py
import sys
sys.path.append("/home/tatchiwiggers/code/batch-1641")

# we take previous functions we wrote
from taxifare.interface.main import evaluate, preprocess, train
from prefect import task, flow

@task
def preprocess_new_data(min_date: str, max_date: str):
    # takes the dates as parameters for one month of data
    preprocess(min_date=min_date, max_date=max_date)

@task
def evaluate_production_model(min_date: str, max_date: str):
    # we call the evaluate function to evaluate the production model
    # the evaluate functions calls the 'load_model' function so that it loads
    #  the in production model so we are evaluating the production model(our default stage is production)
    eval_mae = evaluate(min_date=min_date, max_date=max_date)
    return eval_mae

@task
def re_train(min_date: str, max_date: str):
    # retrains model on new data
    # updates the split ratio to take 20% of the new month as a validation set
    train_mae = train(min_date=min_date, max_date=max_date, split_ratio=0.2)
    return train_mae

@flow
def train_flow():
    # realistically this will be a function calculation the time period
    min_date = "2015-01-01"
    max_date = "2015-02-01"
    processed = preprocess_new_data.submit(min_date, max_date)
    old_mae = evaluate_production_model.submit(min_date, max_date, wait_for=[processed])
    new_mae = re_train.submit(min_date, max_date, wait_for=[processed])

if __name__ == "__main__":
    train_flow()
