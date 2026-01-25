import os
import pandas as pd
from langchain_experimental.agents.agent_toolkits import create_csv_agent
from langchain_openai import ChatOpenAI

# ==========================================
# CONFIGURATION
# ==========================================
# 1. Set your OpenAI API Key
# Ideally, set this as an environment variable for security: export OPENAI_API_KEY="sk-..."
os.environ["OPENAI_API_KEY"] = "your-sk-api-key-here"

# 2. Define the path to your CSV file
CSV_FILE_PATH = "sales_data.csv"

# ==========================================
# PREPARATION: Create a Dummy CSV (For Demo)
# ==========================================
# If the file doesn't exist, we create a sample one so you can run this script immediately.
if not os.path.exists(CSV_FILE_PATH):
    data = {
        "Date": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04"],
        "Product": ["Apple", "Banana", "Apple", "Orange"],
        "Region": ["North", "South", "North", "East"],
        "Sales": [100, 150, 120, 80],
        "Profit": [20, 30, 25, 10]
    }
    df = pd.DataFrame(data)
    df.to_csv(CSV_FILE_PATH, index=False)
    print(f"Created dummy file: {CSV_FILE_PATH}")

# ==========================================
# MAIN LOGIC: The CSV Agent
# ==========================================
def main():
    print("Initializing AI Agent...")
    
    # 1. Initialize the LLM (Language Model)
    # temperature=0 means "be precise, don't be creative" (good for data analysis)
    llm = ChatOpenAI(model="gpt-3.5-turbo", temperature=0)

    # 2. Create the CSV Agent
    # This agent uses the LLM to write Pandas code behind the scenes
    agent = create_csv_agent(
        llm,
        CSV_FILE_PATH,
        verbose=True, # Set to True to see the "thought process" of the AI
        allow_dangerous_code=True # Required because the agent executes python code
    )

    print(f"Agent ready! You can now chat with '{CSV_FILE_PATH}'.")
    print("Type 'exit' to quit.\n")

    # 3. Chat Loop
    while True:
        user_question = input("You: ")
        
        if user_question.lower() in ["exit", "quit", "q"]:
            print("Goodbye!")
            break
            
        try:
            # 4. Ask the agent
            response = agent.run(user_question)
            print(f"AI: {response}\n")
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()