from google import genai
from google.genai import types
from PIL import Image

client = genai.Client()


# prompt = input("Enter the prompt : ")
image = Image.open(r"C:\Users\punitkumar.more\Documents\gcp_de\AUTOMATION\GCP_DE\GenAI\gemini-vid\images\cat.jfif")

# reponse = client.models.generate_content(
reponse = client.models.generate_content_stream(
    model = 'gemini-2.5-flash',
    # contents = prompt,
    contents = [image, "Describe the image in less than 30 words."],
    config = types.GenerateContentConfig(
        system_instruction = "your response should be more than 300 words be funny" ,
        temperature = 2
    )
)

for chunk in reponse:
    print(chunk.text, end='', flush=True)

# print("-----"*10)
# print(reponse.text)
# print("-----"*10)

