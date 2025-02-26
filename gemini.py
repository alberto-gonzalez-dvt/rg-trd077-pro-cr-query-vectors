from langchain.prompts import PromptTemplate
from langchain_google_vertexai import ChatVertexAI
from langchain_core.output_parsers import StrOutputParser
from langchain_google_vertexai import HarmBlockThreshold, HarmCategory

def generate_answer(question, contexts):

  prompt_template = """
    You are an advanced AI assistant tasked with retrieving the most relevant information from a vast database of documents. Given a user query, search through the database and return the most relevant excerpts, summaries, or structured data points. Follow these steps to ensure accuracy and completeness:
      1.- Understand the Query: Analyze the input query carefully to determine its key topics, context, and intent.
      2.- Retrieve Relevant Documents: Search the database and extract the most pertinent documents based on semantic similarity and keyword relevance.
      3.- Extract Key Information: Identify and summarize the most important sections from the retrieved documents that directly answer the user’s query.
      4.- Present a Structured Response: Provide a clear, concise response with supporting details. If necessary, include citations or document references for verification.
      5.- Handle Ambiguity: If the query is unclear, infer the most likely intent and return the best-matching results. If multiple interpretations exist, present alternative results or request clarification."

    Start of the provided set of documents:
        {context}
    End of the provided set of documents.

    Human: {question}
    Chatbot:
        """

  prompt = PromptTemplate(
    input_variables=["context", "question"],
    template=prompt_template
  )

  llm = ChatVertexAI(
    model="gemini-1.5-flash-001",
    temperature=0,
    safety_settings = {
  HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
  HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
  HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
  HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
}
  )

  #Build context form list
  all_context = "\n".join([f'"{context}"' for context in contexts])

  chain = prompt | llm | StrOutputParser()
  res = chain.invoke(input={"context": all_context, "question": question})

  return res