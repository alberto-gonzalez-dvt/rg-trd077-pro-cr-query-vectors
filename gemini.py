from langchain.prompts import PromptTemplate, ChatPromptTemplate
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

def generate_KeyWords(question):
  """Given a text, generate some key words from it using Gemini

  Args:
      question (string): text to extract key words from

  Returns:
      dict: dictionay containing a list of key words
  """  

  json_schema = {
      "title": "keyword_extraction",
      "description": "Extracts the most relevant keywords from a given text.",# and determines whether hybrid search is needed.",
      "type": "object",
      "properties": {
          "key_words": {
              "type": "array",
              "description": "The most relevant keywords extracted from the text for effective search.",
              "items":{ "type": "string" },
          },
          #"hybrid_search": {
          #    "type": "boolean",
          #    "description": "Indicates whether a hybrid search should be performed (True) or not (False)."
          #}
      },
      "required": ["key_words"]#, "hybrid_search"]
  }

  prompt = ChatPromptTemplate.from_template(
    """
    You are an advanced AI assistant asked to find a list of key words in a sentence.
    Given the following text, extract the most relevant keywords that would enable an effective search on the internet or in a database.
    The keywords should:
      1. Be the most representative and meaningful terms from the text.
      2. EXCLUDE terms that could be too general: "definition", "document", "information" or any other generic term.
      3. Include proper nouns, important concepts, and technical terms if present.
      4. Exclude stop words such as articles, prepositions, and pronouns.
      5. Exclude verbs.
      6. Prioritize terms that have the greatest impact on the text's meaning.

    Examples:
      1.- input_text: Where is located the Madrid?
          response: {{key_words:[Madrid]}}
      2.- input_text: What equipment has a vibration level of 10 rms?
          response: {{key_words: [vibration level, 10 rms, equipment]}}
      3.- input_text: What does table mean?
          response: {{key_words: [table]}}
      4.- input_text: What is the definition of table?
          response: {{key_words: [table]}}

    Text: {INPUT_TEXT}
        """
  )   

  llm = ChatVertexAI(
    model="gemini-2.0-flash-lite-001",
    temperature=0,
    safety_settings = {
      HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
      HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
      HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
      HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
    }
  )

  structured_llm = llm.with_structured_output(json_schema, method="json_mode")
  chain_keyword_structured = prompt | structured_llm

  res = chain_keyword_structured.invoke(input={"INPUT_TEXT": question})

  return res