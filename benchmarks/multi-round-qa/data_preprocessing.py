import argparse
import json
import os

import numpy as np
from transformers import AutoTokenizer

parser = argparse.ArgumentParser(description="Process data percentage.")
parser.add_argument(
    "--parse",
    type=float,
    default=1,
    help="The percentage of data to process (0 to 1). Default is 1 (100%).",
)

args = parser.parse_args()

with open("ShareGPT_V3_unfiltered_cleaned_split.json", "r", encoding="utf-8") as file:
    data = json.load(file)


def estimate_num_tokens(text: str) -> int:
    if not hasattr(estimate_num_tokens, "tokenizer"):
        os.environ["TOKENIZERS_PARALLELISM"] = "false"
        estimate_num_tokens.tokenizer = AutoTokenizer.from_pretrained(
            "mistralai/Mistral-7B-Instruct-v0.2"
        )
    return len(estimate_num_tokens.tokenizer.tokenize(text))


def expand_user_message(text: str, should_expand: bool = True) -> str:
    """擴展用戶消息，隨機添加段落直到超過 1500 tokens"""
    if not should_expand:
        return text
        
    current_tokens = estimate_num_tokens(text)
    
    if current_tokens >= 1500:
        return text
    
    # 多個不同的段落，每句一段
    expansion_paragraphs = [
        "This is additional context information to provide more comprehensive background and details for the conversation.",
        "The following text contains relevant information that helps establish context and provides necessary background knowledge for understanding the main topic of discussion.",
        "This context includes various aspects and considerations that are important for a complete understanding of the subject matter.",
        "Additional details and explanations are provided to ensure clarity and comprehensiveness in the conversation flow.",
        "The conversation context is enriched with supplementary information that enhances the overall understanding of the discussed topics.",
        "Various perspectives and viewpoints are included to provide a well-rounded context for the ongoing discussion.",
        "Background information and contextual details are incorporated to facilitate better comprehension of the subject matter.",
        "Supporting context and relevant details are added to ensure the conversation maintains its depth and relevance.",
        "The expanded context includes additional insights and information that contribute to a more comprehensive understanding.",
        "Supplementary context and background details are provided to enhance the overall quality and depth of the conversation."
    ]
    
    import random
    expanded_text = text
    
    # 隨機選擇段落，不斷添加直到超過 1500 tokens
    while estimate_num_tokens(expanded_text) < 1500:
        # 隨機選擇一個段落
        random_paragraph = random.choice(expansion_paragraphs)
        expanded_text += " " + random_paragraph
    
    return expanded_text


num_of_ids = len(data)
print(f"Number of IDs: {num_of_ids}")
data = data[: int(num_of_ids * args.parse)]

count = 0

for d in data:
    d["num_round"] = len(d["conversations"])  # human is one round, gpt is another round
    human_tokens = []
    gpt_tokens = []
    # 檢查這個用戶是否有第一句人類消息
    first_human_found = False
    
    for conv in d["conversations"]:
        if conv["from"] == "human":
            # 只對每個用戶的第一句人類消息進行擴展
            should_expand = not first_human_found
            first_human_found = True
            
            original_text = conv["value"]
            expanded_text = expand_user_message(original_text, should_expand)
            conv["value"] = expanded_text
            if should_expand:
                conv["original_value"] = original_text  # 只對擴展的消息保留原始文本
                print(f"Expanding first message for user {d.get('id', 'unknown')}")
            human_tokens.append(estimate_num_tokens(expanded_text))
        if conv["from"] == "gpt":
            token_number = estimate_num_tokens(conv["value"])
            conv["num_tokens"] = token_number
            gpt_tokens.append(token_number)
    if len(human_tokens) == 0:
        d["average_human_token"] = 0
        d["max_human_token"] = 0
    else:
        d["average_human_token"] = float(np.mean(human_tokens))
        d["max_human_token"] = float(np.max(human_tokens))
    if len(gpt_tokens) == 0:
        d["average_gpt_token"] = 0
        d["max_gpt_token"] = 0
    else:
        d["average_gpt_token"] = float(np.mean(gpt_tokens))
        d["max_gpt_token"] = float(np.max(gpt_tokens))

    count += 1
    print(f"Finished {count}")

# Remove the data that has two consecutive human rounds
del data[260]

with open("ShareGPT.json", "w", encoding="utf-8") as file:
    json.dump(data, file, ensure_ascii=False, indent=2)
