## A CLASS THAT IS GOING TO REPRESENT THE SPAR-SUBMIT INSTRUCTION AND ITS GOING TO EXECUTE IT
## TODO MAYBE IS GOOD TO CREATE A SUBMIT CLASS OUTSIDE OF THE SPARK SCOPE AND MAKE THIS ONE TO INHERIT


class SparkSubmitter:

    def __init__(self):
        pass

    def submit(self,class_in_jar, class_params, jar, master, submit_conf,scheduler_options):
        raise NotImplementedError( "Should have implemented this" )