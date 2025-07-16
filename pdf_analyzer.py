import streamlit as st
import pandas as pd
import json
import os
from datetime import datetime
import io
import time
import logging
from dataclasses import dataclass, field
from pathlib import Path
import re
from typing import List, Dict, Tuple, Optional
import math
import shutil
import hashlib

try:
    import PyPDF2
except ImportError:
    st.error("PyPDF2 non installé. Exécutez: pip install PyPDF2")
    st.stop()

try:
    import openai
except ImportError:
    st.error("OpenAI non installé. Exécutez: pip install openai")
    st.stop()

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    st.warning("python-dotenv non installé. Les variables .env ne seront pas chargées.")

st.set_page_config(
    page_title="MVP Analyse PDF avec IA - Version batch et Excel avancée",
    page_icon="📄",
    layout="wide",
    initial_sidebar_state="expanded"
)

AI_MODELS = {
    'gpt-4.1': {
        'name': 'GPT-4.1',
        'total_tokens': 1048576,
        'max_output': 32768,
        'max_input': 1015808,
        'default_output': 32768,
        'model_id': 'gpt-4.1',
        'supports_vision': True,
        'cost_tier': 'premium'
    },
    'gpt-4.1-mini': {
        'name': 'GPT-4.1 Mini',
        'total_tokens': 1048576,
        'max_output': 32768,
        'max_input': 1015808,
        'default_output': 32768,
        'model_id': 'gpt-4.1-mini',
        'supports_vision': True,
        'cost_tier': 'standard'
    },
    'gpt-4o': {
        'name': 'GPT-4o',
        'total_tokens': 128000,
        'max_output': 16384,
        'max_input': 111616,
        'default_output': 16384,
        'model_id': 'gpt-4o',
        'supports_vision': True,
        'cost_tier': 'standard'
    }
}

@dataclass
class AnalysisResult:
    type: str
    part: str
    chapter: str
    sub_id: str
    text: str
    page: int
    context: str = None
    source_pdf: str = None
    batch_id: str = None
    apa_reference: str = None
    bibliography_entries: List[Dict] = field(default_factory=list)
    text_with_reference: str = None

@dataclass
class PDFChunk:
    content: str
    start_page: int
    end_page: int
    chunk_index: int
    total_chunks: int
    overlap_content: str = None

@dataclass
class BatchRequest:
    custom_id: str
    method: str
    url: str
    body: Dict

class FileManager:
    def __init__(self):
        self.exports_dir = Path("Exports")
        self.backup_dir = Path("Exports/Backup")
        self.ensure_directories()
    
    def ensure_directories(self):
        self.exports_dir.mkdir(exist_ok=True)
        self.backup_dir.mkdir(exist_ok=True)
    
    def get_json_name_key(self, json_file_name: str) -> str:
        return Path(json_file_name).stem[:30].replace(" ", "_")
    
    def generate_excel_filename(self, json_name: str, custom_name: str = None) -> str:
        json_key = self.get_json_name_key(json_name)
        
        if custom_name:
            if json_key not in custom_name:
                custom_name = f"{json_key}_{custom_name}"
            return f"{custom_name[:30].replace(' ', '_')}.xlsx"
        else:
            return f"{json_key}.xlsx"
    
    def get_excel_path(self, json_name: str, excel_filename: str) -> Path:
        json_key = self.get_json_name_key(json_name)
        json_dir = self.exports_dir / json_key
        json_dir.mkdir(exist_ok=True)
        return json_dir / excel_filename
    
    def check_existing_file(self, json_name: str) -> Tuple[bool, List[Path]]:
        json_key = self.get_json_name_key(json_name)
        json_dir = self.exports_dir / json_key
        
        if not json_dir.exists():
            return False, []
        
        existing_files = []
        for file in json_dir.glob("*.xlsx"):
            if json_key in file.stem:
                existing_files.append(file)
        
        return len(existing_files) > 0, existing_files
    
    def create_backup(self, file_path: Path) -> Path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"{timestamp}_{file_path.name}"
        backup_path = self.backup_dir / backup_name
        
        shutil.copy2(file_path, backup_path)
        return backup_path
    
    def load_existing_excel(self, file_path: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
        try:
            with pd.ExcelFile(file_path) as xls:
                df_extracts = pd.read_excel(xls, sheet_name='Extraits') if 'Extraits' in xls.sheet_names else pd.DataFrame()
                df_bibliography = pd.read_excel(xls, sheet_name='Bibliographie') if 'Bibliographie' in xls.sheet_names else pd.DataFrame()
            return df_extracts, df_bibliography
        except Exception as e:
            st.error(f"Erreur lors du chargement du fichier Excel: {str(e)}")
            return pd.DataFrame(), pd.DataFrame()

class PDFAnalyzer:
    def __init__(self):
        self.setup_logging()
        self.max_pages_per_chunk = 15
        self.overlap_pages = 0.5
        self.file_manager = FileManager()
        
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('pdf_analysis.log')
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def extract_text_from_pdf(self, pdf_file) -> Tuple[str, Dict[int, str]]:
        try:
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            full_text = ""
            page_texts = {}
            
            for page_num, page in enumerate(pdf_reader.pages, 1):
                text = page.extract_text()
                page_texts[page_num] = text
                full_text += f"\n--- Page {page_num} ---\n{text}\n"
            
            return full_text, page_texts
            
        except Exception as e:
            self.logger.error(f"Erreur lors de l'extraction PDF: {str(e)}")
            raise
    
    def detect_document_structure(self, page_texts: Dict[int, str]) -> List[Dict]:
        structure = []
        
        title_patterns = [
            r'^(CHAPITRE|CHAPTER)\s+\d+',
            r'^(PARTIE|PART)\s+[IVX]+',
            r'^\d+\.\s+[A-ZÀ-Ÿ]',
            r'^[IVX]+\.\s+[A-ZÀ-Ÿ]',
            r'^#{1,6}\s+',
            r'^\d+\.\d+\s+[A-ZÀ-Ÿ]',
            r'^[A-ZÀ-Ÿ][A-ZÀ-Ÿ\s]{5,}$',
            r'^\s*(INTRODUCTION|CONCLUSION|RÉSUMÉ|ABSTRACT|BIBLIOGRAPHY|RÉFÉRENCES)',
        ]
        
        for page_num, text in page_texts.items():
            lines = text.split('\n')
            for i, line in enumerate(lines[:15]):
                line_clean = line.strip()
                if len(line_clean) < 5 or len(line_clean) > 100:
                    continue
                    
                for pattern in title_patterns:
                    if re.match(pattern, line_clean, re.IGNORECASE):
                        structure.append({
                            'page': page_num,
                            'title': line_clean,
                            'line_num': i,
                            'confidence': self._calculate_title_confidence(line_clean)
                        })
                        break
        
        structure = sorted(structure, key=lambda x: (x['page'], -x['confidence']))
        return structure
    
    def _calculate_title_confidence(self, line: str) -> float:
        confidence = 0.0
        
        if re.match(r'^\d+\.', line):
            confidence += 0.3
        if line.isupper() and len(line) > 10:
            confidence += 0.4
        if any(word in line.lower() for word in ['chapitre', 'partie', 'section', 'introduction', 'conclusion']):
            confidence += 0.5
        if len(line.split()) <= 8:
            confidence += 0.2
        
        return min(confidence, 1.0)
    
    def create_intelligent_chunks(self, page_texts: Dict[int, str]) -> List[PDFChunk]:
        total_pages = len(page_texts)
        
        structure = self.detect_document_structure(page_texts)
        
        if len(structure) >= 2:
            self.logger.info(f"Découpage sémantique: {len(structure)} sections détectées")
            chunks = self._create_semantic_chunks_fixed(page_texts, structure)
            
            theoretical_adaptive_chunks = math.ceil(total_pages / self.max_pages_per_chunk)
            
            if len(chunks) <= theoretical_adaptive_chunks * 1.5:
                self.logger.info(f"Découpage sémantique validé: {len(chunks)} chunks")
                return chunks
            else:
                self.logger.info(f"Découpage sémantique inefficace, passage au découpage adaptatif")
        
        self.logger.info("Utilisation du découpage adaptatif avec chevauchement")
        chunks = self._create_adaptive_chunks_with_overlap(page_texts, 1, total_pages)
        
        for chunk in chunks:
            chunk.total_chunks = len(chunks)
        
        self.logger.info(f"Découpage final: {len(chunks)} chunks pour {total_pages} pages")
        return chunks
    
    def _create_semantic_chunks_fixed(self, page_texts: Dict[int, str], structure: List[Dict]) -> List[PDFChunk]:
        chunks = []
        total_pages = len(page_texts)
        
        current_chunk_start = 1
        current_sections = []
        
        for i, section in enumerate(structure):
            section_start = section['page']
            section_end = structure[i + 1]['page'] - 1 if i + 1 < len(structure) else total_pages
            section_pages = section_end - section_start + 1
            
            if section_pages > self.max_pages_per_chunk:
                if current_sections:
                    chunk_end = current_sections[-1]['end_page']
                    chunk_content = self._extract_pages_content(page_texts, current_chunk_start, chunk_end)
                    overlap_content = self._get_overlap_content(page_texts, chunk_end, total_pages)
                    
                    chunks.append(PDFChunk(
                        content=chunk_content,
                        start_page=current_chunk_start,
                        end_page=chunk_end,
                        chunk_index=len(chunks) + 1,
                        total_chunks=0,
                        overlap_content=overlap_content
                    ))
                    current_sections = []
                
                section_chunks = self._create_adaptive_chunks_with_overlap(
                    page_texts, section_start, section_end
                )
                chunks.extend(section_chunks)
                current_chunk_start = section_end + 1
            else:
                section['end_page'] = section_end
                current_sections.append(section)
                
                chunk_pages = current_sections[-1]['end_page'] - current_chunk_start + 1
                
                if chunk_pages > self.max_pages_per_chunk:
                    last_section = current_sections.pop()
                    
                    if current_sections:
                        chunk_end = current_sections[-1]['end_page']
                        chunk_content = self._extract_pages_content(page_texts, current_chunk_start, chunk_end)
                        overlap_content = self._get_overlap_content(page_texts, chunk_end, total_pages)
                        
                        chunks.append(PDFChunk(
                            content=chunk_content,
                            start_page=current_chunk_start,
                            end_page=chunk_end,
                            chunk_index=len(chunks) + 1,
                            total_chunks=0,
                            overlap_content=overlap_content
                        ))
                    
                    current_chunk_start = last_section['page']
                    current_sections = [last_section]
        
        if current_sections:
            chunk_end = current_sections[-1]['end_page']
            chunk_content = self._extract_pages_content(page_texts, current_chunk_start, chunk_end)
            overlap_content = self._get_overlap_content(page_texts, chunk_end, total_pages)
            
            chunks.append(PDFChunk(
                content=chunk_content,
                start_page=current_chunk_start,
                end_page=chunk_end,
                chunk_index=len(chunks) + 1,
                total_chunks=0,
                overlap_content=overlap_content
            ))
        
        return chunks
    
    def _create_adaptive_chunks_with_overlap(self, page_texts: Dict[int, str], 
                                           start_page: int, end_page: int) -> List[PDFChunk]:
        chunks = []
        current_page = start_page
        
        while current_page <= end_page:
            chunk_end = min(current_page + self.max_pages_per_chunk - 1, end_page)
            
            chunk_content = self._extract_pages_content(page_texts, current_page, chunk_end)
            overlap_content = self._get_overlap_content(page_texts, chunk_end, end_page)
            
            chunks.append(PDFChunk(
                content=chunk_content,
                start_page=current_page,
                end_page=chunk_end,
                chunk_index=len(chunks) + 1,
                total_chunks=0,
                overlap_content=overlap_content
            ))
            
            current_page = chunk_end + 1
        
        return chunks
    
    def _get_overlap_content(self, page_texts: Dict[int, str], last_page: int, total_pages: int) -> str:
        if last_page >= total_pages:
            return None
        
        next_page = last_page + 1
        if next_page in page_texts:
            next_page_text = page_texts[next_page]
            lines = next_page_text.split('\n')
            half_lines = lines[:len(lines)//2]
            return f"\n--- Aperçu page {next_page} ---\n" + '\n'.join(half_lines)
        
        return None
    
    def _extract_pages_content(self, page_texts: Dict[int, str], start: int, end: int) -> str:
        content = ""
        for page_num in range(start, end + 1):
            if page_num in page_texts:
                content += f"\n--- Page {page_num} ---\n{page_texts[page_num]}\n"
        return content
    
    def extract_apa_references(self, text: str) -> List[Dict]:
        references = []
        
        citation_patterns = [
            r'\(([A-Za-z\s&,]+),\s*(\d{4})[,\s]*p?\.?\s*(\d+)?\)',
            r'([A-Za-z\s&]+)\s*\((\d{4})[,\s]*p?\.?\s*(\d+)?\)',
            r'selon\s+([A-Za-z\s&]+)\s*\((\d{4})\)',
            r'd\'après\s+([A-Za-z\s&]+)\s*\((\d{4})\)',
            r'([A-Za-z\s&]+)\s*et\s*al\.\s*\((\d{4})\)',
        ]
        
        for pattern in citation_patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                author = match.group(1).strip()
                year = match.group(2)
                page = match.group(3) if len(match.groups()) >= 3 and match.group(3) else None
                
                references.append({
                    'author': author,
                    'year': year,
                    'page': page,
                    'short_ref': f"({author}, {year}" + (f", p. {page})" if page else ")")
                })
        
        return references
    
    def prepare_batch_requests(self, chunks: List[PDFChunk], plan_json: str, 
                             prompt_template: str, model: str, temperature: float, 
                             top_p: float, max_tokens: int) -> List[BatchRequest]:
        batch_requests = []
        model_config = AI_MODELS[model]
        
        for i, chunk in enumerate(chunks):
            chunk_info = f"\n[CHUNK {chunk.chunk_index}/{chunk.total_chunks} - Pages {chunk.start_page} à {chunk.end_page}]\n"
            
            chunk_prompt = prompt_template.replace('{PLAN_JSON}', plan_json)
            chunk_prompt = chunk_prompt.replace('{PDF_TEXT}', chunk_info + chunk.content)
            
            if chunk.overlap_content:
                chunk_prompt += f"\n[CONTEXTE SUIVANT POUR CONTINUITÉ]\n{chunk.overlap_content}"
            
            batch_request = BatchRequest(
                custom_id=f"chunk_{i}_{chunk.chunk_index}",
                method="POST",
                url="/v1/chat/completions",
                body={
                    "model": model_config['model_id'],
                    "messages": [
                        {"role": "system", "content": "Vous êtes un expert en analyse documentaire académique spécialisé dans l'extraction exhaustive d'informations."},
                        {"role": "user", "content": chunk_prompt}
                    ],
                    "temperature": temperature,
                    "top_p": top_p,
                    "max_tokens": max_tokens
                }
            )
            
            batch_requests.append(batch_request)
        
        return batch_requests
    
    def call_openai_api(self, prompt: str, model: str, temperature: float, 
                       top_p: float, max_tokens: int, api_key: str) -> list:
        try:
            client = openai.OpenAI(api_key=api_key)
            model_config = AI_MODELS[model]
            
            response = client.chat.completions.create(
                model=model_config['model_id'],
                messages=[
                    {"role": "system", "content": "Vous êtes un expert en analyse documentaire académique spécialisé dans l'extraction exhaustive d'informations."},
                    {"role": "user", "content": prompt}
                ],
                temperature=temperature,
                top_p=top_p,
                max_tokens=max_tokens
            )
            
            content = response.choices[0].message.content.strip()
            
            if content.startswith('```json'):
                content = content[7:-3]
            elif content.startswith('```'):
                content = content[3:-3]
            
            try:
                parsed_json = json.loads(content)
                return parsed_json
            except json.JSONDecodeError as e:
                self.logger.error(f"Erreur de parsing JSON: {str(e)}")
                return []
            
        except Exception as e:
            self.logger.error(f"Erreur API OpenAI: {str(e)}")
            raise
    
    def call_openai_batch_api(self, batch_requests: List[BatchRequest], api_key: str, 
                            progress_callback=None) -> List[dict]:
        try:
            client = openai.OpenAI(api_key=api_key)
            
            batch_lines = []
            for request in batch_requests:
                batch_line = {
                    "custom_id": request.custom_id,
                    "method": request.method,
                    "url": request.url,
                    "body": request.body
                }
                batch_lines.append(json.dumps(batch_line))
            
            jsonl_content = "\n".join(batch_lines)
            
            if progress_callback:
                progress_callback(5, 100, "Upload du fichier batch...")
            
            file_obj = io.BytesIO(jsonl_content.encode('utf-8'))
            uploaded_file = client.files.create(
                file=file_obj,
                purpose="batch"
            )
            
            self.logger.info(f"Fichier batch uploadé: {uploaded_file.id}")
            
            if progress_callback:
                progress_callback(10, 100, "Création du batch...")
            
            batch_job = client.batches.create(
                input_file_id=uploaded_file.id,
                endpoint="/v1/chat/completions",
                completion_window="24h"
            )
            
            batch_id = batch_job.id
            self.logger.info(f"Batch créé: {batch_id}")
            
            if progress_callback:
                progress_callback(15, 100, f"Batch {batch_id} en attente de traitement...")
            
            max_wait_time = 24 * 3600
            poll_interval = 60
            elapsed_time = 0
            
            while elapsed_time < max_wait_time:
                try:
                    batch_status = client.batches.retrieve(batch_id)
                    status = batch_status.status
                    
                    if hasattr(batch_status, 'request_counts') and batch_status.request_counts:
                        request_counts = batch_status.request_counts
                        total = getattr(request_counts, 'total', len(batch_requests))
                        completed = getattr(request_counts, 'completed', 0)
                        if total > 0:
                            completion_pct = min(90, int((completed / total) * 75) + 15)
                        else:
                            completion_pct = min(90, int(elapsed_time / max_wait_time * 75) + 15)
                    else:
                        completion_pct = min(90, int(elapsed_time / max_wait_time * 75) + 15)
                    
                    if progress_callback:
                        progress_callback(completion_pct, 100, f"Batch {batch_id}: {status}")
                    
                    if status == "completed":
                        self.logger.info(f"Batch {batch_id} terminé avec succès")
                        break
                    elif status == "failed":
                        error_msg = f"Batch failed: {getattr(batch_status, 'errors', 'Unknown error')}"
                        self.logger.error(error_msg)
                        raise Exception(error_msg)
                    elif status == "expired":
                        self.logger.warning(f"Batch {batch_id} expiré")
                        break
                    elif status == "cancelled":
                        raise Exception(f"Batch {batch_id} a été annulé")
                    
                    time.sleep(poll_interval)
                    elapsed_time += poll_interval
                    
                except Exception as e:
                    if "batch" in str(e).lower():
                        raise
                    else:
                        self.logger.warning(f"Erreur temporaire lors du polling: {str(e)}")
                        time.sleep(poll_interval)
                        elapsed_time += poll_interval
            
            if elapsed_time >= max_wait_time:
                self.logger.warning("Timeout atteint, vérification du statut final...")
                batch_status = client.batches.retrieve(batch_id)
                if batch_status.status not in ["completed", "expired"]:
                    raise Exception("Timeout: Batch n'a pas été complété dans le délai de 24h")
            
            if progress_callback:
                progress_callback(90, 100, "Récupération des résultats...")
            
            if not hasattr(batch_status, 'output_file_id') or not batch_status.output_file_id:
                raise Exception("Aucun fichier de résultats disponible")
            
            result_file_id = batch_status.output_file_id
            result_content = client.files.content(result_file_id)
            
            results = []
            error_count = 0
            
            for line in result_content.text.strip().split('\n'):
                if not line.strip():
                    continue
                    
                try:
                    result_data = json.loads(line)
                    
                    if result_data.get('error'):
                        error_count += 1
                        self.logger.warning(f"Erreur pour {result_data.get('custom_id')}: {result_data.get('error')}")
                        continue
                    
                    response_body = result_data.get('response', {}).get('body', {})
                    if response_body.get('choices'):
                        content = response_body['choices'][0]['message']['content']
                        
                        content = content.strip()
                        if content.startswith('```json'):
                            content = content[7:-3]
                        elif content.startswith('```'):
                            content = content[3:-3]
                        
                        try:
                            parsed_content = json.loads(content)
                            if isinstance(parsed_content, list):
                                results.extend(parsed_content)
                            else:
                                results.append(parsed_content)
                        except json.JSONDecodeError as je:
                            self.logger.error(f"Erreur parsing JSON pour {result_data.get('custom_id')}: {str(je)}")
                            error_count += 1
                            
                except json.JSONDecodeError as e:
                    self.logger.error(f"Erreur parsing ligne résultat: {str(e)}")
                    error_count += 1
                    continue
            
            if progress_callback:
                progress_callback(100, 100, f"Batch complété! {len(results)} résultats, {error_count} erreurs")
            
            self.logger.info(f"Batch terminé: {len(results)} résultats extraits, {error_count} erreurs")
            
            return results
            
        except Exception as e:
            self.logger.error(f"Erreur API Batch OpenAI: {str(e)}")
            raise
    
    def integrate_apa_reference_in_text(self, text: str, apa_reference: str) -> str:
        if not apa_reference:
            return text
        
        return f"{text}\u00A0{apa_reference}"
    
    def analyze_pdf_chunks(self, pdf_file, chunks: List[PDFChunk], plan_json: str, 
                          prompt_template: str, model: str, temperature: float, 
                          top_p: float, max_tokens: int, api_key: str, 
                          use_batch_mode: bool = False, progress_callback=None) -> Tuple[List[AnalysisResult], List[Dict]]:
        all_results = []
        all_bibliography = []
        bibliography_set = set()
        
        if use_batch_mode:
            if progress_callback:
                progress_callback(0, 100, "Préparation des requêtes batch...")
            
            batch_requests = self.prepare_batch_requests(
                chunks, plan_json, prompt_template, model, temperature, top_p, max_tokens
            )
            
            api_results = self.call_openai_batch_api(batch_requests, api_key, progress_callback)
            
            for result in api_results:
                analysis_references = self.extract_apa_references(result.get('text', ''))
                
                apa_ref = result.get('apa_reference')
                original_text = result.get('text', '')
                text_with_ref = self.integrate_apa_reference_in_text(original_text, apa_ref)
                
                analysis_result = AnalysisResult(
                    type=result.get('type', 'analysis'),
                    part=result.get('part', ''),
                    chapter=result.get('chapter', ''),
                    sub_id=result.get('sub_id', ''),
                    text=original_text,
                    page=result.get('page', 1),
                    context=result.get('context'),
                    source_pdf=pdf_file.name if hasattr(pdf_file, 'name') else 'unknown',
                    apa_reference=apa_ref,
                    bibliography_entries=result.get('bibliography_entries', []),
                    text_with_reference=text_with_ref
                )
                
                for entry in result.get('bibliography_entries', []):
                    entry_key = f"{entry.get('author', '')}_{entry.get('year', '')}"
                    if entry_key not in bibliography_set:
                        bibliography_set.add(entry_key)
                        all_bibliography.append(entry)
                
                all_results.append(analysis_result)
        else:
            for i, chunk in enumerate(chunks):
                if progress_callback:
                    progress_callback(i, len(chunks), f"Analyse exhaustive du chunk {i+1}/{len(chunks)}")
                
                chunk_info = f"\n[CHUNK {chunk.chunk_index}/{chunk.total_chunks} - Pages {chunk.start_page} à {chunk.end_page}]\n"
                
                chunk_prompt = prompt_template.replace('{PLAN_JSON}', plan_json)
                chunk_prompt = chunk_prompt.replace('{PDF_TEXT}', chunk_info + chunk.content)
                
                if chunk.overlap_content:
                    chunk_prompt += f"\n[CONTEXTE SUIVANT POUR CONTINUITÉ]\n{chunk.overlap_content}"
                
                api_results = self.call_openai_api(
                    chunk_prompt, model, temperature, top_p, max_tokens, api_key
                )
                
                chunk_references = self.extract_apa_references(chunk.content)
                
                for result in api_results:
                    analysis_references = self.extract_apa_references(result.get('text', ''))
                    
                    apa_ref = result.get('apa_reference')
                    original_text = result.get('text', '')
                    text_with_ref = self.integrate_apa_reference_in_text(original_text, apa_ref)
                    
                    analysis_result = AnalysisResult(
                        type=result.get('type', 'analysis'),
                        part=result.get('part', ''),
                        chapter=result.get('chapter', ''),
                        sub_id=result.get('sub_id', ''),
                        text=original_text,
                        page=result.get('page', 1),
                        context=result.get('context'),
                        source_pdf=pdf_file.name if hasattr(pdf_file, 'name') else 'unknown',
                        apa_reference=apa_ref,
                        bibliography_entries=result.get('bibliography_entries', []),
                        text_with_reference=text_with_ref
                    )
                    
                    for entry in result.get('bibliography_entries', []):
                        entry_key = f"{entry.get('author', '')}_{entry.get('year', '')}"
                        if entry_key not in bibliography_set:
                            bibliography_set.add(entry_key)
                            all_bibliography.append(entry)
                    
                    all_results.append(analysis_result)
        
        return all_results, all_bibliography

EXHAUSTIVE_PROMPT = """# === Rôle et objectif général ==========================
Vous êtes un agent d'analyse documentaire expert en supervision humaine de l'IA et en rédaction académique.
Votre tâche CRITIQUE : analyser EXHAUSTIVEMENT le chunk de PDF fourni et générer TOUS les extraits pertinents avec références APA complètes.

⚠️ **IMPÉRATIF D'EXHAUSTIVITÉ** : Vous devez identifier et extraire TOUS les éléments pertinents du chunk, même les plus subtils. Aucune information utile ne doit être omise.

# === Contexte fourni =====================================================
## PLAN_JSON
{PLAN_JSON}
## PDF_TEXT
{PDF_TEXT}

# === Règles CRITIQUES pour l'analyse EXHAUSTIVE ======================================

1. **EXHAUSTIVITÉ ABSOLUE - PRIORITÉ MAXIMALE**
   - Analysez CHAQUE paragraphe, CHAQUE section du chunk
   - Extrayez TOUTES les informations qui correspondent au plan éditorial
   - Identifiez les connexions subtiles et implications indirectes
   - Ne négligez AUCUN détail pertinent, même secondaire
   - Préférez extraire trop plutôt que pas assez

2. **PROFONDEUR ANALYTIQUE OBLIGATOIRE**
   - Chaque analyse doit contenir AU MINIMUM 50-100 mots
   - Inclure : contexte détaillé, interprétation approfondie, implications, liens avec le plan
   - Intégrer une réflexion critique et mise en perspective
   - Expliquer la PERTINENCE de chaque extrait par rapport au plan

3. **RÉFÉRENCES APA SYSTÉMATIQUES**
   - Si le PDF cite une source → extraire et formater en APA
   - Si vous mentionnez une source dans votre analyse → référence APA complète
   - Format court dans le texte : (Auteur, année, p. X)
   - Référence complète dans bibliography_entries

4. **STRUCTURE DE SORTIE JSON**
   Chaque entrée DOIT contenir :
   ```json
   {
     "type": "citation" | "analysis",
     "part": "I",
     "chapter": "3",
     "sub_id": "3.2",
     "text": "[50-100 mots minimum pour les analyses approfondies, citation exacte entre « » pour les citations]",
     "page": 42,
     "context": "[Contexte très détaillé : qui parle, dans quel cadre, quelle section du document]",
     "apa_reference": "(Auteur, 2023, p. 15)",
     "bibliography_entries": [
       {
         "author": "Nom, P.",
         "year": "2023",
         "title": "Titre complet de l'ouvrage ou article",
         "publisher": "Éditeur",
         "url": "https://...",
         "type": "book|article|report|website"
       }
     ]
   }
   ```

5. **EXTRACTION SYSTÉMATIQUE DE TOUS LES ÉLÉMENTS**
   - Citations directes et indirectes
   - Analyses et interprétations
   - Données statistiques et chiffres
   - Exemples et cas d'étude
   - Recommandations et conclusions
   - Méthodologies et approches
   - Comparaisons et contrastes
   - Implications théoriques et pratiques

6. **QUALITÉ DES ANALYSES EXHAUSTIVES**
   Chaque analyse doit obligatoirement inclure :
   - Le CONTEXTE très précis (chapitre/section du PDF, auteur, cadre)
   - L'INTERPRÉTATION approfondie (signification multiple, nuances)
   - Les IMPLICATIONS (conséquences directes et indirectes, enjeux)
   - Le LIEN explicite avec le plan éditorial fourni
   - La PERTINENCE justifiée pour le sujet global

7. **VIGILANCE POUR LES ÉLÉMENTS SUBTILS**
   - Notes de bas de page importantes
   - Références en passant mais significatives
   - Implications non explicites mais importantes
   - Connexions entre concepts
   - Nuances dans le ton et l'argumentation

# === Instructions finales ================================================
- Analysez INTÉGRALEMENT le chunk fourni ligne par ligne
- Produisez des analyses substantielles et exhaustives (50-100 mots minimum)
- Extrayez TOUTES les références mentionnées
- Incluez TOUS les éléments pertinents même marginalement
- Format de sortie : JSON strict uniquement
- OBJECTIF : Zéro perte d'information pertinente

SORTEZ UNIQUEMENT LE JSON, sans autre texte."""

def init_session_state():
    if 'results' not in st.session_state:
        st.session_state.results = []
    if 'bibliography' not in st.session_state:
        st.session_state.bibliography = []
    if 'logs' not in st.session_state:
        st.session_state.logs = []
    if 'batch_counter' not in st.session_state:
        st.session_state.batch_counter = 0
    if 'current_json_name' not in st.session_state:
        st.session_state.current_json_name = None

def add_log(message: str, level: str = "info"):
    timestamp = datetime.now().strftime("%H:%M:%S")
    st.session_state.logs.append({
        'timestamp': timestamp,
        'level': level,
        'message': message
    })

def format_apa_bibliography(entry: Dict) -> str:
    author = entry.get('author', 'Auteur inconnu')
    year = entry.get('year', 's.d.')
    title = entry.get('title', 'Sans titre')
    publisher = entry.get('publisher', '')
    url = entry.get('url', '')
    entry_type = entry.get('type', 'book')
    
    apa_format = f"{author} ({year}). "
    
    if entry_type == 'article':
        apa_format += f"{title}. "
    else:
        apa_format += f"*{title}*. "
    
    if publisher:
        apa_format += f"{publisher}. "
    
    if url:
        apa_format += f"Récupéré de {url}"
    
    return apa_format.strip()

def check_duplicate_entries(existing_df: pd.DataFrame, new_results: List[AnalysisResult]) -> List[AnalysisResult]:
    if existing_df.empty:
        return new_results
    
    existing_signatures = set()
    for _, row in existing_df.iterrows():
        signature = hashlib.md5(f"{row.get('Texte', '')}{row.get('Page', '')}".encode()).hexdigest()
        existing_signatures.add(signature)
    
    filtered_results = []
    for result in new_results:
        signature = hashlib.md5(f"{result.text}{result.page}".encode()).hexdigest()
        if signature not in existing_signatures:
            filtered_results.append(result)
    
    return filtered_results

def export_to_excel_with_complete_bibliography(results: List[AnalysisResult], 
                                             bibliography: List[Dict],
                                             file_path: Path,
                                             completion_mode: bool = False) -> bytes:
    
    extracts_data = []
    for result in results:
        extracts_data.append({
            'Type': result.type,
            'Partie': result.part,
            'Chapitre': result.chapter,
            'Sous-partie': result.sub_id,
            'Texte': result.text,
            'Page': result.page,
            'Contexte': result.context or '',
            'Source PDF': result.source_pdf,
            'Référence APA': result.apa_reference or '',
            'Texte avec référence': result.text_with_reference or result.text,
            'Batch ID': result.batch_id
        })
    
    bibliography_data = []
    for entry in bibliography:
        bibliography_data.append({
            'Référence APA complète': format_apa_bibliography(entry),
            'Type': entry.get('type', ''),
            'Année': entry.get('year', ''),
            'Auteur': entry.get('author', '')
        })
    
    df_extracts = pd.DataFrame(extracts_data) if extracts_data else pd.DataFrame()
    df_bibliography = pd.DataFrame(bibliography_data) if bibliography_data else pd.DataFrame()
    
    if completion_mode and file_path.exists():
        try:
            temp_analyzer = PDFAnalyzer()
            existing_extracts, existing_bibliography = temp_analyzer.file_manager.load_existing_excel(file_path)
            
            filtered_results = check_duplicate_entries(existing_extracts, results)
            add_log(f"Doublons éliminés: {len(results) - len(filtered_results)} sur {len(results)}")
            
            if filtered_results:
                filtered_extracts_data = []
                for result in filtered_results:
                    filtered_extracts_data.append({
                        'Type': result.type,
                        'Partie': result.part,
                        'Chapitre': result.chapter,
                        'Sous-partie': result.sub_id,
                        'Texte': result.text,
                        'Page': result.page,
                        'Contexte': result.context or '',
                        'Source PDF': result.source_pdf,
                        'Référence APA': result.apa_reference or '',
                        'Texte avec référence': result.text_with_reference or result.text,
                        'Batch ID': result.batch_id
                    })
                df_extracts = pd.DataFrame(filtered_extracts_data)
            else:
                df_extracts = pd.DataFrame()
            
            if not existing_extracts.empty and not df_extracts.empty:
                df_extracts = pd.concat([existing_extracts, df_extracts], ignore_index=True)
            elif not existing_extracts.empty:
                df_extracts = existing_extracts
            
            if not existing_bibliography.empty and not df_bibliography.empty:
                df_bibliography = pd.concat([existing_bibliography, df_bibliography], ignore_index=True)
                df_bibliography = df_bibliography.drop_duplicates(subset=['Référence APA complète'])
            elif not existing_bibliography.empty:
                df_bibliography = existing_bibliography
                
        except Exception as e:
            add_log(f"Erreur lors de la fusion: {str(e)}", "error")
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_extracts.to_excel(writer, sheet_name='Extraits', index=False)
        df_bibliography.to_excel(writer, sheet_name='Bibliographie', index=False)
        
        for sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                if column_letter in ['E', 'J']:
                    adjusted_width = min(max_length + 2, 150)
                elif column_letter == 'A':
                    adjusted_width = min(max_length + 2, 120)
                else:
                    adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width
    
    return output.getvalue()

def load_api_key():
    api_key = os.getenv('OPENAI_API_KEY')
    if api_key:
        return api_key
    return None

def main():
    init_session_state()
    analyzer = PDFAnalyzer()
    
    st.title("📄 Analyse PDF avec IA - Version batch et Excel avancée")
    st.markdown("### Analyse exhaustive avec mode batch et gestion Excel intelligente")
    
    st.info("""
    🆕 **Nouvelles fonctionnalités** :
    - 🚀 **Mode batch OpenAI** : API officielle avec 50% de réduction de coût
    - 📝 **Référence intégrée** : Référence courte ajoutée automatiquement après chaque texte
    - 📁 **Gestion Excel avancée** : Un fichier par JSON avec nommage intelligent
    - 🔄 **Mode complétion** : Détection automatique et fusion avec fichiers existants
    - 💾 **Sauvegarde automatique** : Backup avant toute modification
    """)
    
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        st.markdown("#### API OpenAI")
        api_key_from_env = load_api_key()
        
        if api_key_from_env:
            st.success("✅ Clé API chargée depuis .env")
            api_key = api_key_from_env
            st.text("Clé API: " + api_key[:10] + "..." + api_key[-4:])
        else:
            st.warning("⚠️ Clé API non trouvée dans .env")
            api_key = st.text_input("Clé API manuelle", type="password", help="Votre clé API OpenAI")
        
        st.markdown("#### 🚀 Mode de traitement")
        use_batch_mode = st.checkbox(
            "Activer le mode batch OpenAI",
            value=False,
            help="API Batch officielle OpenAI : 50% moins cher, mais traitement jusqu'à 24h"
        )
        
        if use_batch_mode:
            st.warning("""
            ⚠️ **Mode Batch API OpenAI** :
            - ✅ **50% moins cher** que l'API standard
            - ✅ **Limites de taux plus élevées** (pool séparé)
            - ⏰ **Traitement asynchrone** : peut prendre **plusieurs heures** (jusqu'à 24h)
            - 📊 **Maximum 50,000 requêtes** par batch
            - 💾 **Fichier de 200 MB maximum**
            
            ⚠️ **Important** : Le traitement n'est PAS immédiat ! Vous devrez attendre que OpenAI traite votre batch.
            """)
        else:
            st.info("ℹ️ Mode unitaire : Requêtes envoyées une par une (plus rapide mais plus cher)")
        
        if use_batch_mode and st.session_state.get('total_chunks_estimated', 0) > 20:
            st.error(f"""
            🚨 **Attention** : {st.session_state.get('total_chunks_estimated', 0)} chunks détectés en mode batch !
            
            Le traitement pourrait prendre **plusieurs heures**. Considérez :
            - Réduire le nombre de fichiers PDF
            - Augmenter la taille des chunks
            - Ou utiliser le mode unitaire pour un résultat immédiat
            """)
        
        st.markdown("#### Sélection du modèle")
        selected_model = st.selectbox(
            "Modèle IA",
            options=list(AI_MODELS.keys()),
            format_func=lambda x: AI_MODELS[x]['name'],
            index=0
        )
        
        col1, col2 = st.columns(2)
        with col1:
            temperature = st.slider("Température", 0.0, 2.0, 0.5, 0.1)
        with col2:
            top_p = st.slider("Top P", 0.0, 1.0, 1.0, 0.1)
        
        st.markdown("#### Configuration du découpage")
        st.info("🤖 **Découpage automatique intelligent** :\n1. Tentative sémantique (structure)\n2. Si échec → adaptatif avec chevauchement 0.5 page")
        
        max_pages = st.number_input(
            "Pages max par chunk",
            min_value=10,
            max_value=25,
            value=15,
            help="Nombre maximum de pages par chunk (recommandé: 15)"
        )
        analyzer.max_pages_per_chunk = max_pages
        
        model_info = AI_MODELS[selected_model]
        max_output_tokens = st.number_input(
            "Max Output Tokens", 
            value=model_info['default_output'],
            max_value=model_info['max_output'],
            help="Tokens maximum pour la réponse (défaut: maximum du modèle)"
        )
        
        st.markdown("#### ℹ️ Informations modèle")
        st.text(f"Tokens totaux: {model_info['total_tokens']:,}")
        st.text(f"Input max: {model_info['max_input']:,}")
        st.text(f"Output max: {model_info['max_output']:,}")
    
    tab1, tab2, tab3, tab4 = st.tabs(["📁 Fichiers", "📝 Plan & Prompt", "🚀 Analyse", "📊 Résultats"])
    
    with tab1:
        st.header("Gestion des fichiers")
        
        uploaded_files = st.file_uploader(
            "Sélectionnez les fichiers PDF",
            type=['pdf'],
            accept_multiple_files=True,
            help="Sélectionnez un ou plusieurs fichiers PDF à analyser"
        )
        
        if uploaded_files:
            st.success(f"✅ {len(uploaded_files)} fichier(s) PDF chargé(s)")
            
            for file in uploaded_files:
                size_mb = file.size / (1024 * 1024)
                st.write(f"📄 {file.name} ({size_mb:.1f} MB)")
    
    with tab2:
        st.header("Plan éditorial et Prompt")
        
        col1, col2 = st.columns([1, 1])
        
        with col1:
            st.markdown("#### Plan éditorial (JSON)")
            
            plan_file = st.file_uploader("Charger un plan JSON", type=['json'])
            
            if plan_file:
                try:
                    plan_content = json.load(plan_file)
                    st.session_state.editorial_plan = json.dumps(plan_content, indent=2, ensure_ascii=False)
                    st.session_state.current_json_name = plan_file.name
                    st.success("✅ Plan éditorial chargé")
                except:
                    st.error("❌ Erreur JSON")
            
            if 'editorial_plan' not in st.session_state:
                default_plan_path = 'plan.json'
                if os.path.exists(default_plan_path):
                    try:
                        with open(default_plan_path, 'r', encoding='utf-8') as f:
                            st.session_state.editorial_plan = f.read()
                        st.session_state.current_json_name = "plan.json"
                        st.success("✅ Plan par défaut chargé")
                    except:
                        st.session_state.editorial_plan = '{"plan": "Veuillez charger un plan éditorial"}'
                        st.session_state.current_json_name = "default.json"
                else:
                    st.session_state.editorial_plan = '{"plan": "Veuillez charger un plan éditorial"}'
                    st.session_state.current_json_name = "default.json"
            
            editorial_plan = st.text_area(
                "Contenu du plan éditorial",
                value=st.session_state.editorial_plan,
                height=400
            )
            
            if st.session_state.current_json_name:
                st.info(f"📝 JSON actuel: {st.session_state.current_json_name}")
        
        with col2:
            st.markdown("#### Prompt d'analyse exhaustive")
            
            prompt_template = st.text_area(
                "Template du prompt (optimisé pour l'exhaustivité)",
                value=EXHAUSTIVE_PROMPT,
                height=400,
                help="Prompt optimisé pour une analyse exhaustive sans perte d'information"
            )
    
    with tab3:
        st.header("Lancement de l'analyse exhaustive")
        
        can_analyze = True
        checks = []
        
        if not api_key:
            checks.append("❌ Clé API manquante")
            can_analyze = False
        else:
            checks.append("✅ Clé API disponible")
        
        if not uploaded_files:
            checks.append("❌ Aucun fichier PDF")
            can_analyze = False
        else:
            checks.append(f"✅ {len(uploaded_files)} fichier(s) PDF")
        
        if not st.session_state.current_json_name:
            checks.append("❌ Aucun plan JSON")
            can_analyze = False
        else:
            checks.append(f"✅ Plan JSON: {st.session_state.current_json_name}")
        
        for check in checks:
            st.write(check)
        
        if can_analyze and st.session_state.current_json_name:
            st.markdown("### 📁 Gestion des fichiers Excel")
            
            has_existing, existing_files = analyzer.file_manager.check_existing_file(st.session_state.current_json_name)
            
            if has_existing:
                st.warning(f"⚠️ Fichier(s) Excel existant(s) détecté(s) pour '{st.session_state.current_json_name}'")
                
                for file_path in existing_files:
                    size_mb = file_path.stat().st_size / (1024 * 1024)
                    mod_time = datetime.fromtimestamp(file_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
                    st.write(f"📁 {file_path.name} ({size_mb:.1f} MB, modifié le {mod_time})")
                
                col1, col2 = st.columns(2)
                with col1:
                    completion_mode = st.radio(
                        "Mode de traitement",
                        ["Compléter le fichier existant", "Créer un nouveau fichier"],
                        help="Compléter : ajouter au fichier existant | Nouveau : créer un fichier séparé"
                    )
                
                with col2:
                    if completion_mode == "Créer un nouveau fichier":
                        custom_filename = st.text_input(
                            "Nom personnalisé (optionnel)",
                            placeholder="nouveau_analysis",
                            help="Laissez vide pour un nom automatique"
                        )
                    else:
                        st.info("💾 Sauvegarde automatique avant modification")
            else:
                st.success("✅ Aucun fichier existant - nouveau fichier sera créé")
                completion_mode = "Créer un nouveau fichier"
                custom_filename = st.text_input(
                    "Nom du fichier Excel (optionnel)",
                    placeholder="",
                    help="Laissez vide pour utiliser le nom automatique basé sur le JSON"
                )
        
        col1, col2 = st.columns(2)
        
        with col1:
            preview_clicked = st.button("🔍 Prévisualiser l'analyse", disabled=not can_analyze)
        
        with col2:
            direct_analysis_clicked = st.button("🚀 Démarrer l'analyse directe", disabled=not can_analyze)
        
        if preview_clicked and can_analyze:
            preview_info = []
            total_chunks_estimated = 0
            
            with st.spinner("📊 Analyse préliminaire des fichiers..."):
                for uploaded_file in uploaded_files:
                    try:
                        full_text, page_texts = analyzer.extract_text_from_pdf(uploaded_file)
                        chunks = analyzer.create_intelligent_chunks(page_texts)
                        
                        total_chunks_estimated += len(chunks)
                        structure = analyzer.detect_document_structure(page_texts)
                        
                        preview_info.append({
                            'file': uploaded_file.name,
                            'pages': len(page_texts),
                            'chunks': len(chunks),
                            'method': 'Sémantique' if len(structure) >= 2 else 'Adaptatif',
                            'sections': len(structure)
                        })
                        
                    except Exception as e:
                        st.error(f"Erreur lors de l'analyse de {uploaded_file.name}: {str(e)}")
                        st.stop()
            
            st.session_state.preview_info = preview_info
            st.session_state.total_chunks_estimated = total_chunks_estimated
            st.session_state.show_preview = True
        
        if st.session_state.get('show_preview', False):
            preview_info = st.session_state.get('preview_info', [])
            total_chunks_estimated = st.session_state.get('total_chunks_estimated', 0)
            
            st.markdown("### 📊 Prévisualisation de l'analyse")
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total de chunks", total_chunks_estimated)
            with col2:
                total_pages = sum([info['pages'] for info in preview_info])
                st.metric("Total de pages", total_pages)
            with col3:
                st.metric("Fichiers", len(preview_info))
            with col4:
                avg_pages_per_chunk = total_pages / total_chunks_estimated if total_chunks_estimated > 0 else 0
                st.metric("Pages/chunk moyen", f"{avg_pages_per_chunk:.1f}")
            
            if use_batch_mode:
                st.info("🚀 Mode batch activé - Traitement groupé des requêtes")
            else:
                st.info("⚡ Mode unitaire activé - Requêtes séquentielles")
            
            st.markdown("#### 📋 Détail par fichier")
            df_preview = pd.DataFrame(preview_info)
            df_preview.columns = ['Fichier', 'Pages', 'Chunks', 'Méthode', 'Sections détectées']
            st.dataframe(df_preview, use_container_width=True)
            
            st.markdown("#### ⚙️ Validation des paramètres")
            
            param_checks = []
            
            if total_chunks_estimated > 50:
                param_checks.append(("⚠️", f"Nombre de chunks élevé ({total_chunks_estimated}). Cela peut prendre du temps."))
            elif total_chunks_estimated > 100:
                param_checks.append(("❌", f"Nombre de chunks très élevé ({total_chunks_estimated}). Considérez augmenter la taille des chunks."))
            else:
                param_checks.append(("✅", f"Nombre de chunks acceptable ({total_chunks_estimated})"))
            
            model_info = AI_MODELS[selected_model]
            if model_info['cost_tier'] == 'premium':
                param_checks.append(("💰", f"Modèle premium sélectionné ({model_info['name']}) - coût élevé"))
            else:
                param_checks.append(("✅", f"Modèle sélectionné: {model_info['name']}"))
            
            if temperature > 0.7:
                param_checks.append(("⚠️", f"Température élevée ({temperature}) - résultats plus créatifs mais moins précis"))
            else:
                param_checks.append(("✅", f"Température optimale ({temperature})"))
            
            estimated_tokens = total_chunks_estimated * max_output_tokens
            if estimated_tokens > 1000000:
                param_checks.append(("💰", f"Utilisation estimée: {estimated_tokens:,} tokens - coût potentiellement élevé"))
            else:
                param_checks.append(("✅", f"Utilisation estimée: {estimated_tokens:,} tokens"))
            
            if use_batch_mode and total_chunks_estimated > 100:
                param_checks.append(("⚠️", "Mode batch avec nombreux chunks - traitement très long possible"))
            elif use_batch_mode:
                param_checks.append(("✅", "Mode batch optimal pour ce volume"))
            
            for emoji, message in param_checks:
                st.write(f"{emoji} {message}")
            
            st.markdown("---")
            confirm_clicked = st.button("🚀 Confirmer et lancer l'analyse complète", type="primary", key="confirm_analysis")
            
            if confirm_clicked:
                st.session_state.start_analysis = True
                st.session_state.show_preview = False
                st.rerun()
        
        if direct_analysis_clicked or st.session_state.get('start_analysis', False):
            st.session_state.start_analysis = False
            
            st.markdown("### 🚀 Lancement de l'analyse exhaustive confirmée")
            
            json_name = st.session_state.current_json_name
            is_completion = False
            excel_filename = ""
            
            if can_analyze and json_name:
                has_existing, existing_files = analyzer.file_manager.check_existing_file(json_name)
                
                if has_existing and 'completion_mode' in locals():
                    if completion_mode == "Compléter le fichier existant":
                        is_completion = True
                        excel_filename = existing_files[0].name
                        backup_path = analyzer.file_manager.create_backup(existing_files[0])
                        add_log(f"Backup créé: {backup_path}")
                        st.success(f"💾 Backup créé: {backup_path.name}")
                    else:
                        custom_name = locals().get('custom_filename', '')
                        excel_filename = analyzer.file_manager.generate_excel_filename(json_name, custom_name)
                else:
                    custom_name = locals().get('custom_filename', '')
                    excel_filename = analyzer.file_manager.generate_excel_filename(json_name, custom_name)
            
            st.session_state.results = []
            st.session_state.bibliography = []
            
            batch_id = f"batch_{int(time.time())}"
            add_log(f"Démarrage du batch exhaustif {batch_id} ({'batch' if use_batch_mode else 'unitaire'})")
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            total_files = len(uploaded_files)
            
            for file_idx, uploaded_file in enumerate(uploaded_files):
                try:
                    status_text.text(f"📄 Extraction du texte de {uploaded_file.name}...")
                    full_text, page_texts = analyzer.extract_text_from_pdf(uploaded_file)
                    
                    status_text.text(f"🧠 Découpage intelligent automatique...")
                    chunks = analyzer.create_intelligent_chunks(page_texts)
                    
                    structure = analyzer.detect_document_structure(page_texts)
                    chunk_method = "sémantique" if len(structure) >= 2 else "adaptatif"
                    add_log(f"Découpage {chunk_method}: {len(chunks)} chunks créés pour {uploaded_file.name}")
                    
                    def update_progress(chunk_idx, total_chunks, message):
                        if use_batch_mode:
                            file_progress = file_idx / total_files
                            batch_progress = chunk_idx / 100 / total_files
                            total_progress = file_progress + batch_progress
                        else:
                            file_progress = file_idx / total_files
                            chunk_progress = chunk_idx / total_chunks / total_files
                            total_progress = file_progress + chunk_progress
                        
                        progress_bar.progress(min(total_progress, 1.0))
                        status_text.text(f"🔍 {message}")
                    
                    results, bibliography = analyzer.analyze_pdf_chunks(
                        uploaded_file,
                        chunks,
                        editorial_plan,
                        prompt_template,
                        selected_model,
                        temperature,
                        top_p,
                        max_output_tokens,
                        api_key,
                        use_batch_mode=use_batch_mode,
                        progress_callback=update_progress
                    )
                    
                    for result in results:
                        result.batch_id = batch_id
                    
                    st.session_state.results.extend(results)
                    st.session_state.bibliography.extend(bibliography)
                    
                    mode_text = "batch" if use_batch_mode else "unitaire"
                    add_log(f"✅ {uploaded_file.name}: {len(results)} extraits, {len(bibliography)} références (méthode: {chunk_method}, mode: {mode_text})")
                    
                except Exception as e:
                    add_log(f"❌ Erreur avec {uploaded_file.name}: {str(e)}", "error")
                    st.error(f"Erreur lors de l'analyse de {uploaded_file.name}: {str(e)}")
            
            progress_bar.progress(1.0)
            status_text.text("✅ Analyse exhaustive terminée !")
            
            if excel_filename and json_name:
                try:
                    excel_path = analyzer.file_manager.get_excel_path(json_name, excel_filename)
                    excel_data = export_to_excel_with_complete_bibliography(
                        st.session_state.results,
                        st.session_state.bibliography,
                        excel_path,
                        completion_mode=is_completion
                    )
                    
                    with open(excel_path, 'wb') as f:
                        f.write(excel_data)
                    
                    mode_text = "complété" if is_completion else "créé"
                    st.success(f"💾 Fichier Excel {mode_text}: {excel_path}")
                    add_log(f"Fichier Excel {mode_text}: {excel_path}")
                    
                except Exception as e:
                    st.error(f"Erreur lors de la sauvegarde Excel: {str(e)}")
                    add_log(f"❌ Erreur sauvegarde Excel: {str(e)}", "error")
            
            total_extracts = len(st.session_state.results)
            total_citations = len([r for r in st.session_state.results if r.type == 'citation'])
            total_analyses = len([r for r in st.session_state.results if r.type == 'analysis'])
            total_refs = len(st.session_state.bibliography)
            
            col1, col2, col3, col4, col5 = st.columns(5)
            with col1:
                st.metric("Total extraits", total_extracts)
            with col2:
                st.metric("Citations", total_citations)
            with col3:
                st.metric("Analyses", total_analyses)
            with col4:
                st.metric("Références", total_refs)
            with col5:
                mode_icon = "🚀" if use_batch_mode else "⚡"
                st.metric(f"{mode_icon} Mode", "Batch" if use_batch_mode else "Unitaire")
            
            mode_text = "batch" if use_batch_mode else "unitaire"
            add_log(f"🎉 Analyse exhaustive terminée (mode {mode_text}): {total_extracts} extraits, {total_refs} références bibliographiques")
        
        if st.session_state.logs:
            st.markdown("#### 📋 Journal d'activité")
            log_container = st.container()
            with log_container:
                for log in st.session_state.logs[-10:]:
                    level_emoji = {
                        'info': 'ℹ️',
                        'error': '❌'
                    }.get(log['level'], 'ℹ️')
                    st.text(f"[{log['timestamp']}] {level_emoji} {log['message']}")
    
    with tab4:
        st.header("Résultats de l'analyse exhaustive")
        
        if st.session_state.results:
            col1, col2, col3, col4, col5, col6 = st.columns(6)
            
            with col1:
                st.metric("Total extraits", len(st.session_state.results))
            with col2:
                citations = len([r for r in st.session_state.results if r.type == 'citation'])
                st.metric("Citations", citations)
            with col3:
                analyses = len([r for r in st.session_state.results if r.type == 'analysis'])
                st.metric("Analyses", analyses)
            with col4:
                sources = len(set([r.source_pdf for r in st.session_state.results]))
                st.metric("Sources PDF", sources)
            with col5:
                st.metric("Références biblio", len(st.session_state.bibliography))
            with col6:
                with_ref = len([r for r in st.session_state.results if r.apa_reference])
                st.metric("Avec référence", with_ref)
            
            col1, col2 = st.columns([3, 1])
            with col2:
                if st.button("📥 Exporter Excel complet", type="primary"):
                    temp_path = Path("temp_export.xlsx")
                    excel_data = export_to_excel_with_complete_bibliography(
                        st.session_state.results,
                        st.session_state.bibliography,
                        temp_path,
                        completion_mode=False
                    )
                    st.download_button(
                        label="💾 Télécharger Excel",
                        data=excel_data,
                        file_name=f"analyse_exhaustive_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
            
            res_tab1, res_tab2, res_tab3 = st.tabs(["📋 Extraits", "📚 Bibliographie APA", "🔗 Références intégrées"])
            
            with res_tab1:
                col1, col2, col3 = st.columns(3)
                with col1:
                    type_filter = st.selectbox("Type", ["Tous", "citation", "analysis"])
                with col2:
                    sources = ["Tous"] + list(set([r.source_pdf for r in st.session_state.results]))
                    source_filter = st.selectbox("Source", sources)
                with col3:
                    parts = ["Tous"] + list(set([r.part for r in st.session_state.results if r.part]))
                    part_filter = st.selectbox("Partie", parts)
                
                filtered_results = st.session_state.results
                if type_filter != "Tous":
                    filtered_results = [r for r in filtered_results if r.type == type_filter]
                if source_filter != "Tous":
                    filtered_results = [r for r in filtered_results if r.source_pdf == source_filter]
                if part_filter != "Tous":
                    filtered_results = [r for r in filtered_results if r.part == part_filter]
                
                if filtered_results:
                    st.success(f"🔍 **Analyse exhaustive** : {len(filtered_results)} éléments extraits")
                    
                    for i, result in enumerate(filtered_results[:25]):
                        with st.expander(f"{result.type.title()} - {result.sub_id} (p. {result.page}) - Longueur: {len(result.text)} caractères"):
                            st.markdown(f"**Texte:** {result.text}")
                            if result.context:
                                st.markdown(f"**Contexte détaillé:** {result.context}")
                            if result.apa_reference:
                                st.markdown(f"**Référence APA:** {result.apa_reference}")
                            if result.text_with_reference and result.text_with_reference != result.text:
                                st.markdown(f"**Texte avec référence intégrée:** {result.text_with_reference}")
                            st.caption(f"Source: {result.source_pdf} | Batch: {result.batch_id}")
                    
                    if len(filtered_results) > 25:
                        st.info(f"Affichage limité aux 25 premiers résultats sur {len(filtered_results)} (voir Excel pour la liste complète)")
            
            with res_tab2:
                if st.session_state.bibliography:
                    st.markdown("### 📚 Références bibliographiques (format APA complet)")
                    
                    sorted_biblio = sorted(
                        st.session_state.bibliography,
                        key=lambda x: (x.get('author', ''), x.get('year', ''))
                    )
                    
                    for i, ref in enumerate(sorted_biblio, 1):
                        apa_complete = format_apa_bibliography(ref)
                        st.markdown(f"**{i}.** {apa_complete}")
                        
                    st.success(f"✅ {len(sorted_biblio)} références bibliographiques extraites et formatées en APA")
                else:
                    st.info("Aucune référence bibliographique extraite")
            
            with res_tab3:
                st.markdown("### 🔗 Aperçu des textes avec références intégrées")
                
                results_with_ref = [r for r in st.session_state.results if r.apa_reference and r.text_with_reference]
                
                if results_with_ref:
                    st.success(f"📝 {len(results_with_ref)} extraits avec références intégrées (espace insécable)")
                    
                    for result in results_with_ref[:10]:
                        with st.expander(f"Extrait {result.sub_id} (p. {result.page})"):
                            st.markdown("**Texte original:**")
                            st.write(result.text)
                            st.markdown("**Texte avec référence intégrée:**")
                            st.write(result.text_with_reference)
                            st.caption("🔗 La référence est ajoutée avec un espace insécable pour éviter les retours à la ligne")
                    
                    if len(results_with_ref) > 10:
                        st.info(f"Affichage limité aux 10 premiers résultats sur {len(results_with_ref)}")
                else:
                    st.info("Aucun extrait avec référence intégrée trouvé")
        else:
            st.info("🔄 Aucun résultat disponible. Lancez une analyse exhaustive dans l'onglet précédent.")
            
            st.markdown("#### 🎯 Nouvelles fonctionnalités de cette version")
            st.markdown("""
            - **🚀 Mode batch OpenAI** : API officielle avec 50% de réduction de coût (jusqu'à 24h de traitement)
            - **📝 Référence intégrée** : Référence courte automatiquement ajoutée après chaque texte avec espace insécable
            - **📁 Gestion Excel avancée** : Un fichier Excel par plan JSON avec nommage intelligent
            - **🔄 Mode complétion** : Détection automatique des fichiers existants et fusion intelligente
            - **💾 Sauvegarde automatique** : Backup automatique avant toute modification de fichier existant
            - **🛡️ Évitement des doublons** : Détection et élimination automatique des extraits dupliqués
            """)

if __name__ == "__main__":
    main()