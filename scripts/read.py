from pypdf import PdfReader, PdfWriter
import os
import glob
from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential

SECTION_OVERLAP = 100
MAX_SECTION_LENGTH = 1000
SENTENCE_SEARCH_LIMIT = 100

def split_text(pages):
    SENTENCE_ENDINGS = [".", "!", "?"]
    WORDS_BREAKS = [",", ";", ":", " ", "(", ")", "[", "]", "{", "}", "\t", "\n"]

    page_map = []
    offset = 0
    for i, p in enumerate(pages):
        text = p.extract_text()
        page_map.append((i, offset, text))
        offset += len(text)

    def find_page(offset):
        l = len(page_map)
        for i in range(l - 1):
            if offset >= page_map[i][1] and offset < page_map[i + 1][1]:
                return i
        return l - 1

    all_text = "".join(p[2] for p in page_map)
    length = len(all_text)
    start = 0
    end = length
    while start + SECTION_OVERLAP < length:
        last_word = -1
        end = start + MAX_SECTION_LENGTH

        if end > length:
            end = length
        else:
            # Try to find the end of the sentence
            while end < length and (end - start - MAX_SECTION_LENGTH) < SENTENCE_SEARCH_LIMIT and all_text[end] not in SENTENCE_ENDINGS:
                if all_text[end] in WORDS_BREAKS:
                    last_word = end
                end += 1
            if end < length and all_text[end] not in SENTENCE_ENDINGS and last_word > 0:
                end = last_word # Fall back to at least keeping a whole word
        if end < length:
            end += 1

        # Try to find the start of the sentence or at least a whole word boundary
        last_word = -1
        while start > 0 and start > end - MAX_SECTION_LENGTH - 2 * SENTENCE_SEARCH_LIMIT and all_text[start] not in SENTENCE_ENDINGS:
            if all_text[start] in WORDS_BREAKS:
                last_word = start
            start -= 1
        if all_text[start] not in SENTENCE_ENDINGS and last_word > 0:
            start = last_word
        if start > 0:
            start += 1

        yield (all_text[start:end], find_page(start))
        start = end - SECTION_OVERLAP
        
    if start + SECTION_OVERLAP < end:
        yield (all_text[start:end], find_page(start))

def index_sections(filename, sections):
    search_creds = AzureKeyCredential("oQ9DCTktptgdZNUSveD28AC8MSBfmaxnzSytelSXfbAzSeBujVVP".strip())
    search_client = SearchClient(endpoint=f"https://gptkb-fw53u4y2hw7do.search.windows.net/",
                                    index_name="gptkbindex",
                                    credential=search_creds)
    i = 0
    batch = []
    for s in sections:
        batch.append(s)
        #print(s)
        i += 1
        if i % 1000 == 0:
            print("ENTRANDO")
            #print(batch)
    if len(batch) > 0:
        print(batch)
        results = search_client.upload_documents(documents=batch)
        succeeded = sum([1 for r in results if r.succeeded])
        print(f"\tIndexed {len(results)} sections, {succeeded} succeeded")

def blob_name_from_file_page(filename, page):
    return os.path.splitext(os.path.basename(filename))[0] + f"-{page}" + ".pdf"

def create_sections(filename, pages):
    for i, (section, pagenum) in enumerate(split_text(pages)):
        yield {
            "id": f"{filename}-{i}".replace(".", "_").replace(" ", "_"),
            "content": section,
            "sourcepage": blob_name_from_file_page(filename, pagenum),
            "sourcefile": filename
        }

#for filename in glob.glob('./data/*'):
#    reader = PdfReader(filename)
#    pages = reader
#    sections = create_sections(os.path.basename(filename), pages)
#    index_sections(os.path.basename(filename), sections)
#    print(sections)

filename = "./data/RIMAC_Seguros.pdf"
reader = PdfReader(filename)
number_of_pages = len(reader.pages)
pages = reader.pages
sections = create_sections(os.path.basename(filename), pages)
index_sections(os.path.basename(filename), sections)