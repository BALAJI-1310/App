
from notebookutils import mssparkutils

# function to clean spark/java long error
def clean_error(e):
    err = str(e)

    # take only first meaningful line
    err = err.split("\n")[0]

    remove_words = [
        "org.apache.spark.SparkException:",
        "java.lang.RuntimeException:",
        "py4j.protocol.Py4JJavaError:",
        "Job aborted due to stage failure:"
    ]

    for w in remove_words:
        err = err.replace(w, "")

    return err.strip()
