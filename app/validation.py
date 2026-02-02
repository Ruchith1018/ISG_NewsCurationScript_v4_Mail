def clean_escapes(text: str, label: str = "content") -> str:
    if not text:
        print(f"✅ {label} passed validation (empty)")
        return text

    original = text

    # content has escaping so removing it
    text = text.replace("\n\n", "")
    text = text.replace("\n", "")
    text = text.replace("\r", "")
    text = text.replace("\\n", "")
    text = text.replace("\\r", "")
    text = text.replace("\\t", "")

    return text
