from langchain.prompts import PromptTemplate
from langchain_google_vertexai import ChatVertexAI
from langchain_core.output_parsers import StrOutputParser

def generate_answer(question, contexts):

  #prompt_template = """
  #Retrieved contexts:
  #{context}

  #Instruction:
  #You must use the provided contexts to answer the following question accurately and completely. 
  #Always respond in the same language as the question, whether it is in English, Spanish, or any other language.
  #If the answer is not in the provided contexts, clearly state that.

  #Question:
  #{question}

  #Answer (in the same language as the question):
  #"""
  prompt_template = """You are a Bot assistant answering any questions about documents.
    You are given a question and a set of documents. Your tone must be formal and professional. Your answers should be concise and brief.
    Don't confuse documents with chat history. Base your answers only on documents.
    If the user's question requires you to provide specific information from the documents, give your answer based only on the context extracted from. DON'T generate an answer that is NOT written in the provided documents.
    If you don't find the answer to the user's question with the context provided to you below, answer that you didn't find the answer in the documentation and propose him to express his query with more details.
    Provide your whole answer in HTML format without the <head> tag. The language of the answer MUST be the same language of the question.
 
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
    temperature=0
  )

  #Build context form list
  all_context = "\n".join([f'"{context}"' for context in contexts])

  chain = prompt | llm | StrOutputParser()
  res = chain.invoke(input={"context": all_context, "question": question})

  return res