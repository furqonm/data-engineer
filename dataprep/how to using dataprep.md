# How to Use Dataprep - Initial Setup

Lab reference: [Working with Cloud Dataprep on Google Cloud](https://www.skills.google/course_templates/773/labs/499688)

Run these commands in Cloud Shell to set up the environment before opening Cloud Dataprep (Alteryx Designer Cloud) in the console.

## Cloud Shell Setup

```bash
# Set your active project ID
export PROJECT_ID=$(gcloud config get-value project)

# 1. Enable the Dataprep API and create its service identity
gcloud services enable dataprep.googleapis.com
gcloud beta services identity create --service=dataprep.googleapis.com

# 2. Create a Cloud Storage bucket named after your project ID
gcloud storage buckets create gs://$PROJECT_ID --project=$PROJECT_ID

# 3. Copy the lab source files into your bucket
gcloud storage cp -r gs://spls/gsp050 gs://$PROJECT_ID
```

Once the commands finish, open the Google Cloud Console navigation menu and go to **Alteryx Designer Cloud** (Cloud Dataprep) to start working with the dataset from `gs://<PROJECT_ID>/gsp050`.
