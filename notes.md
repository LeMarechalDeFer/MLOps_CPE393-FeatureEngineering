
Lab
• Work on your own dataset
• Discuss with your team 2-3 questions about the data
• Clean and engineer features if necessary
• Build data view to address those questions
• Implement them as the workflow in Airflow (each 
transformation operation should be represented as an 
operator)
• Capture the airflow graph, save as PDF and upload to LEB2


You need to set the following environment variables to run the airflow server:
AIRFLOW_UID (add the user id of your linux user (id -u))
AIRFLOW_IMAGE_NAME (apache/airflow:3.0.0)
_AIRFLOW_WWW_USER_USERNAME
_AIRFLOW_WWW_USER_PASSWORD


You can admin the airflow and flower dashboard at:
http://localhost:5555/
http://localhost:8080/



