udf_functions = []
arrow_udf_functions = []

def udf(func):
    udf_functions.append(func)
    return func

def arrow_udf(func):
    arrow_udf_functions.append(func)
    return func
    
def get_udfs():
    return udf_functions