import pandas as pd
import random

def generate_texts_with_min_words(count=50000, min_words=1000, filename="gen.csv", duplicate_each=True, seed: int | None = 42):
    """生成文本并保存到CSV文件，每个文本至少包含指定数量的单词

    若 duplicate_each=True，則輸出資料會是「相鄰兩行相同」，方便用於 Round Robin 測試
    （請求1與2相同、3與4相同，以此類推）。
    """

    expansion_paragraphs=[
        "This is additional context information carefully crafted to provide a much more comprehensive background, offering detailed insights, explanations, and examples that help to set the stage for the ongoing conversation. It ensures that all relevant aspects, including historical background, key concepts, and potential implications, are thoroughly covered, allowing participants to have a clearer understanding of the subject at hand and facilitating more meaningful discussions.",
        "The following text contains relevant and carefully selected information that helps to establish a clear and solid context. It provides necessary background knowledge, including definitions, related issues, and foundational concepts, which are essential for understanding the main topic of discussion. This detailed context aims to promote effective communication and minimize misunderstandings among all involved parties.",
        "This context encompasses various important aspects and considerations that are crucial for a complete understanding of the subject matter. It includes historical background, key principles, related issues, and potential consequences that might influence or shape the overall discussion. By exploring these elements, participants can develop a more nuanced and informed perspective on the topic.",
        "Additional details and thorough explanations are provided here to ensure maximum clarity and comprehensiveness. This helps participants follow the conversation more easily, reduces the risk of confusion, and addresses potential misunderstandings that could arise without sufficient contextual information. It also offers examples and clarifications to deepen understanding.",
        "The conversation context is further enriched with supplementary information that enhances overall comprehension of the topics being discussed. It offers deeper insights, clarifications, case studies, and relevant examples that support better engagement and understanding, making complex ideas more accessible and easier to grasp for all participants involved.",
        "Various perspectives and viewpoints are thoughtfully included to provide a well-rounded and balanced context for the ongoing discussion. Encouraging consideration of different angles, interpretations, and opinions helps foster a more comprehensive understanding of the subject, promoting critical thinking and open-minded dialogue among participants.",
        "Background information and detailed contextual elements are incorporated to facilitate better comprehension. This includes explaining underlying assumptions, exploring nuanced details, and discussing broader implications. Such comprehensive background helps participants grasp the complexity and interconnectedness of the topics being examined.",
        "Supporting context and relevant details are added to maintain the depth and relevance of the conversation. These elements help connect different ideas, reinforce key points, and build a solid foundation for further discussion, decision-making, or problem-solving, ensuring that conversations stay focused and meaningful.",
        "The expanded context includes additional insights, related examples, historical references, and expert opinions that contribute to a richer and more comprehensive understanding. These elements serve to deepen the overall narrative, making the discussion more engaging, informative, and valuable for all participants.",
        "Supplementary background details and context are provided to elevate the overall quality and depth of the conversation. They make the discussion more informative, meaningful, and engaging by offering relevant data, insights, and examples that support better understanding and facilitate more productive interactions."
    ]

    if seed is not None:
        random.seed(seed)

    unique_texts = []
    for i in range(count):
        # 初始化文本和单词计数
        text_parts = []
        word_count = 0
        
        # 持续添加段落直到达到最小单词数
        while word_count < min_words:
            # 随机选择一个段落
            paragraph = random.choice(expansion_paragraphs)
            text_parts.append(paragraph)
            text_parts.append(paragraph)
            text_parts.append(paragraph)
            text_parts.append(paragraph)
            text_parts.append(paragraph)
            
            # 更新单词计数
            word_count += len(paragraph.split())*5
        
        # 将所有段落连接成一个文本
        full_text = " ".join(text_parts)
        unique_texts.append(full_text)
        
        # 每生成1000个文本打印一次进度
        if (i + 1) % 1000 == 0:
            print(f"Generated {i + 1} texts")
    
    # 構造最終輸出：相鄰兩行相同（若 duplicate_each=True）
    if duplicate_each:
        doubled = []
        for t in unique_texts:
            doubled.append(t)
            doubled.append(t)
        output = doubled
    else:
        output = unique_texts

    # 保存到CSV文件
    df = pd.DataFrame(output, columns=['text'])
    df.to_csv(filename, index=False)
    print(f"Generated and saved {count} texts to {filename}, each with at least {min_words} words")
    
    return output

# 运行生成函数
if __name__ == "__main__":
    # 預設生成相鄰兩行相同的資料
    generate_texts_with_min_words(50000, 1000, "gen.csv", duplicate_each=True)
