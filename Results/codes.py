import uuid
import itertools
import json
import pandas as pd
import requests

url = "https://xrfvuwgjrlznxdhiqded.supabase.co/rest/v1/commdata"

headers = {
    "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InhyZnZ1d2dqcmx6bnhkaGlxZGVkIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NzQ5NzEyNSwiZXhwIjoyMDczMDczMTI1fQ.sFT6MsN7Phla94BIyHXRjiLZB8TLQof9U17Rv51XJaM",
    "apikey": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InhyZnZ1d2dqcmx6bnhkaGlxZGVkIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NzQ5NzEyNSwiZXhwIjoyMDczMDczMTI1fQ.sFT6MsN7Phla94BIyHXRjiLZB8TLQof9U17Rv51XJaM"
}

params = {
    "limit": 100
}

response = requests.get(url, headers=headers, params=params)

if response.status_code == 200:
    data = response.json()
#     print("Retrieved", len(data), "rows")
#     print(data[:5])  # print first 5 rows as sample
# else:
#     print("Error:", response.status_code, response.text)


information = pd.DataFrame(data)
# print(information.info())


df_json = information["raw_content"].apply(json.loads).apply(pd.Series)

df_json = df_json.rename(columns={"id": "content_id"})

df = information.rename(columns={"id": "meta_id"})

final_df = pd.concat([df.drop(columns=["raw_content"]), df_json], axis=1)

dim_comm_type = final_df[["comm_type"]
                         ].drop_duplicates().reset_index(drop=True)
dim_comm_type["comm_type_id"] = dim_comm_type.index + 1
dim_comm_type = dim_comm_type[["comm_type_id", "comm_type"]]

dim_subject = final_df[["subject"]].drop_duplicates().reset_index(drop=True)
dim_subject["subject_id"] = dim_subject.index + 1
dim_subject = dim_subject[["subject_id", "subject"]]

dim_calendar = final_df[["calendar_id"]
                        ].drop_duplicates().reset_index(drop=True)
dim_calendar["calendar_id"] = dim_calendar["calendar_id"]  

dim_audio = final_df[["audio_url"]].drop_duplicates().reset_index(drop=True)
dim_audio["audio_id"] = dim_audio.index + 1
dim_audio = dim_audio[["audio_id", "audio_url"]]

dim_video = final_df[["video_url"]].drop_duplicates().reset_index(drop=True)
dim_video["video_id"] = dim_video.index + 1
dim_video = dim_video[["video_id", "video_url"]]

dim_transcript = final_df[["transcript_url"]
                          ].drop_duplicates().reset_index(drop=True)
dim_transcript["transcript_id"] = dim_transcript.index + 1
dim_transcript = dim_transcript[["transcript_id", "transcript_url"]]


email_sources = []

for col in ["speakers", "participants", "meeting_attendees", "host_email", "organizer_email"]:
    if col in final_df.columns:
        if col in ["speakers", "participants"]:
            email_sources.append(final_df[col].explode())
        elif col == "meeting_attendees":
           
            email_sources.append(
                final_df[col].explode().apply(
                    lambda x: x.get("email") if isinstance(x, dict) else x
                )
            )
        else:  
            email_sources.append(final_df[col])

if email_sources:
    all_emails = (
        pd.concat(email_sources, ignore_index=True)
          .dropna()
          .drop_duplicates()
          .tolist()
    )
else:
    all_emails = []


dim_user = pd.DataFrame({"email": all_emails})
dim_user["user_id"] = [str(uuid.uuid4()) for _ in range(len(dim_user))]




dim_user = dim_user[["user_id", "name", "email",
                     "location", "displayName", "phoneNumber"]]


fact_communication = final_df.merge(dim_comm_type, on="comm_type") \
    .merge(dim_subject, on="subject") \
    .merge(dim_audio, on="audio_url") \
    .merge(dim_video, on="video_url") \
    .merge(dim_transcript, on="transcript_url")

fact_communication = fact_communication[[
    "meta_id", "content_id", "duration", "dateString",
    "comm_type_id", "subject_id", "calendar_id",
    "audio_id", "video_id", "transcript_id"
]]
fact_communication = fact_communication.rename(columns={"meta_id": "comm_id"})


bridge_rows = []

for _, row in final_df.iterrows():
    comm_id = row["meta_id"]

    
    bridge_rows.append((comm_id, row["host_email"], "isHost"))
    bridge_rows.append((comm_id, row["organizer_email"], "isOrganiser"))

    for p in row["participants"]:
        bridge_rows.append((comm_id, p, "isParticipant"))

    for s in row["speakers"]:
        bridge_rows.append((comm_id, s, "isSpeaker"))

    for a in row["meeting_attendees"]:
        bridge_rows.append((comm_id, a["email"], "isAttendee"))

bridge_comm_user = pd.DataFrame(
    bridge_rows, columns=["comm_id", "email", "role"])
bridge_comm_user = bridge_comm_user.merge(dim_user, on="email")[
    ["comm_id", "user_id", "role"]]


with pd.ExcelWriter("result1.xlsx", engine="openpyxl") as writer:
    dim_user.to_excel(writer, sheet_name="dim_user", index=False)
    dim_comm_type.to_excel(writer, sheet_name="dim_comm_type", index=False)
    dim_subject.to_excel(writer, sheet_name="dim_subject", index=False)
    dim_calendar.to_excel(writer, sheet_name="dim_calendar", index=False)
    dim_audio.to_excel(writer, sheet_name="dim_audio", index=False)
    dim_video.to_excel(writer, sheet_name="dim_video", index=False)
    dim_transcript.to_excel(writer, sheet_name="dim_transcript", index=False)
    fact_communication.to_excel(
        writer, sheet_name="fact_communication", index=False)
    bridge_comm_user.to_excel(
        writer, sheet_name="bridge_comm_user", index=False)
