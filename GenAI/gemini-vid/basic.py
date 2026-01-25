from google import genai

client = genai.Client()

prompt = input("Enter your Prompt : ")

response = client.models.generate_content(
    model="gemini-2.5-flash",
    contents = prompt
)

print("The response is", response.text)
