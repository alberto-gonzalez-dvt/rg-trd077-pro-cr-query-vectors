import os
from langchain_core.prompts import PromptTemplate, ChatPromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.output_parsers import StrOutputParser
from langchain_google_genai import HarmBlockThreshold, HarmCategory

# Get env vars
project_id = os.environ.get('project_id')
gemini_location = os.environ.get('reservation_location')
response_gemini_model= os.environ.get("response_gemini_model")
keywords_gemini_model = os.environ.get("keywords_gemini_model")

def init_llm_model(project_id, location, response_gemini_model, temperature, thinking_budget, safety_settings):
    """Init LLM model with its configuration for generation content. Always use vertexai=True to go through Vertex AI service.

    Args:
        project_id (str): project ID to use
        location (str): Gemini endpoint location
        response_gemini_model (_typstre_): gemini version to use
        temperature (float): temperature setting for generating content
        thinking_budget (int): number of tokens to be used when reasoning. 0 = OFF, -1 = dinamyc,  n = number of tokens to use
        safety_settings (dict): level of security and block for differents categories

    Returns:
        ChatGoogleGenerativeAI: langchain class of our LLM to be used
    """
    llm = ChatGoogleGenerativeAI(
        project=project_id,
        location=location,
        vertexai=True,
        model=response_gemini_model,
        temperature=temperature,
        thinking_budget=thinking_budget,
        safety_settings = safety_settings,
        )

    return llm

def generate_answer(question, contexts):

  prompt_template = """
    You are an advanced AI assistant tasked with retrieving the most relevant information from a vast database of documents. Given a user query, search through the database and return the most relevant excerpts, summaries, or structured data points. Follow these steps to ensure accuracy and completeness:
      1.- Understand the Query: Analyze the input query carefully to determine its key topics, context, and intent.
      2.- Retrieve Relevant Documents: Search the database and extract the most pertinent documents based on semantic similarity and keyword relevance.
      3.- Extract Key Information: Identify and summarize the most important sections from the retrieved documents that directly answer the user’s query.
      4.- Present a Structured Response: Provide a clear, concise response with supporting details. If necessary, include citations or document references for verification.
      5.- Handle Ambiguity: If the query is unclear, infer the most likely intent and return the best-matching results. If multiple interpretations exist, present alternative results or request clarification.
      6.- Respond in natural language, without using code blocks, tool code, or JSON formatting.

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

  # Init LLM model
  llm = init_llm_model(project_id=project_id, 
                          location=gemini_location, 
                          response_gemini_model=response_gemini_model, 
                          temperature=0, 
                          thinking_budget=0, 
                          safety_settings={
                            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
                            HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
                            HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
                            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
                            }
                        )

  #Build context form list
  all_context = "\n".join([f'"{context}"' for context in contexts])

  # Build chain and run infer
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
      "description": "Extracts the most relevant keywords from a given text.",
      "type": "object",
      "properties": {
          "key_words": {
              "type": "array",
              "description": "The most relevant keywords extracted from the text for effective search.",
              "items":{ "type": "string" },
          },
      },
      "required": ["key_words"]
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
      1.- input_text: Where is located Madrid?
          response: {{key_words:[Madrid]}}
      2.- input_text: What equipment has a power consumption of 500 watts?
          response: {{key_words: [power consumption, 500 watts, equipment]}}
      3.- input_text: What does table mean?
          response: {{key_words: [table]}}
      4.- input_text: What is the definition of table?
          response: {{key_words: [table]}}

    Text: {INPUT_TEXT}
        """
  )   

  # Init LLM model
  llm = init_llm_model(project_id=project_id, 
                        location=gemini_location, 
                        response_gemini_model=keywords_gemini_model, 
                        temperature=0, 
                        thinking_budget=0, 
                        safety_settings={
                            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
                            HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
                            HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
                            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
                            }
                        )

  # Build chain and run infer
  structured_llm = llm.with_structured_output(json_schema, method="json_mode")
  chain_keyword_structured = prompt | structured_llm

  res = chain_keyword_structured.invoke(input={"INPUT_TEXT": question})

  return res


def generate_keywords_and_weights(question):
  """Gemini call to get key-words and weights given a question. 
    Weights apply to SEARCH and VECTOR_SEARCH results(after re-ranking), to choose the relevance of each type of search for 
    the given question.

  Args:
      question (string): user question

  Returns:
      dict: dictionary containing the list of key-words extracted and the weights for each type of search.
  """  
  
  json_schema = {
      "title": "keyword_extraction_and_weights",
      "description": "Extracts the most relevant keywords from a given text and assign weights to keyword-based search and semantic search results.",
      "type": "object",
      "properties": {
          "keyword_weight": {
              "type": "number",
              "description": "Weight for keyword search results.",
          },
          "semantic_weight": {
              "type": "number",
              "description": "Weight for semantic search results."
          },
          "key_words": {
              "type": "array",
              "description": "The most relevant keywords extracted from the text for effective search.",
              "items":{ "type": "string" },
          },
      },
      "required": ["keyword_weight", "semantic_weight", "key_words"]
  }

  prompt_template = """
You are an advanced AI system that performs two tasks on a user query:
    1. Assign importance weights to two search strategies: keyword-based and semantic-based, based only on the query's content and structure.
    2. Extract the most relevant keywords that would enable an effective search on the internet or in a database.

Your response should be a JSON object containing:
    - `keyword_weight`: A float between 0 and 1 indicating the relevance of keyword-based search.
    - `semantic_weight`: A float between 0 and 1 indicating the relevance of semantic-based search.
    - `key_words`: A list of meaningful and specific keywords extracted from the query.
(Note: `keyword_weight` and `semantic_weight` must sum to 1.)

--------------------------
Search Weighting Rules:

Use higher **keyword_weight** (0.7–1.0) when the query:
    - Contains **acronyms**, **codes**, **part numbers**, or **product names** (e.g. "ISO-548", "USB 3.2 Gen 2").
    - Is short, telegraphic, or command-style.
    - Has strong exact-match terms like brands, models, or specs.
    - Refers to specific known entities (e.g. "error 403", "Canon R5 battery").

Use higher **semantic_weight** (0.7–1.0) when the query:
    - Is abstract, intent-driven, open-ended, or subjective.
    - Uses full grammatical sentences *without specific identifiers*.
    - Expresses uncertainty, intent, opinion, or exploratory behavior.

(Note:Do not use the extracted keywords below when assigning weights. Weights must be determined from the full original query only.)

--------------------------
Keyword Extraction Rules:
    - Extract the most representative and meaningful terms from the text.
    - EXCLUDE overly general words like: definition, document, information, etc.
    - INCLUDE proper nouns, important concepts, technical terms, and specific values.
    - REMOVE stop words (articles, prepositions, pronouns) and verbs.
    - PRIORITIZE terms that carry the greatest impact on meaning.

--------------------------
Examples:
    1. Query: RTX 4070 laptop 32gb ram
       Output: {{keyword_weight: 0.85, semantic_weight: 0.15, key_words: [RTX 4070, laptop, 32gb ram]}}

    2. Query: what is a good laptop for game development?
       Output: {{keyword_weight: 0.2, semantic_weight: 0.8, key_words: [laptop, game development]}}

    3. Query: error 404 unauthorized user
       Output: {{keyword_weight: 0.8, semantic_weight: 0.2, key_words: [error 404, unauthorized, user]}}

    4. Query: how do I fix error 403 in nginx?
       Output: {{keyword_weight: 0.3, semantic_weight: 0.7, key_words: [fix, error 403, nginx]}}

    5. Query: What is the objective of ISO-548 and ISO-547?
       Output: {{keyword_weight: 0.7, semantic_weight: 0.3, key_words: [objective, ISO-548, ISO-547]}}

    6. Query: What is the definition of table?
       Output: {{keyword_weight: 0.2, semantic_weight: 0.8, key_words: [table]}}

--------------------------
Now process this query:
Query: {query}
Output:
"""

  prompt = ChatPromptTemplate.from_template(prompt_template)

  # Init LLM model
  llm = init_llm_model(project_id=project_id, 
                        location=gemini_location, 
                        response_gemini_model=keywords_gemini_model, 
                        temperature=0, 
                        thinking_budget=0, 
                        safety_settings={
                            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
                            HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
                            HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
                            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
                            }
                        )

  # Build chain and run infer
  structured_llm = llm.with_structured_output(json_schema, method="json_mode")
  chain_structured = prompt | structured_llm
  res = chain_structured.invoke(input={"query": question})

  return res