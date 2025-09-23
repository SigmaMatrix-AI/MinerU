import json
import os
import tempfile
import base64
from flask import Flask, request, jsonify
from magic_pdf.api.magic_pdf_parse import pdf_parse_main

app = Flask(__name__)

@app.route('/ping', methods=['GET'])
def ping():
    return jsonify({'status': 'healthy'})

@app.route('/invocations', methods=['POST'])
def invocations():
    try:
        data = request.get_json()
        
        # 获取PDF文件（base64编码）
        pdf_base64 = data.get('pdf_base64')
        if not pdf_base64:
            return jsonify({'error': 'Missing pdf_base64 parameter'}), 400
        
        # 解码PDF文件
        pdf_data = base64.b64decode(pdf_base64)
        
        # 创建临时文件
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_pdf:
            temp_pdf.write(pdf_data)
            temp_pdf_path = temp_pdf.name
        
        # 创建输出目录
        output_dir = tempfile.mkdtemp()
        
        try:
            # 调用MinerU解析PDF
            result = pdf_parse_main(
                pdf_path=temp_pdf_path,
                output_dir=output_dir,
                method="auto"
            )
            
            # 读取解析结果
            output_files = os.listdir(output_dir)
            results = {}
            
            for file in output_files:
                file_path = os.path.join(output_dir, file)
                if file.endswith('.md'):
                    with open(file_path, 'r', encoding='utf-8') as f:
                        results['markdown'] = f.read()
                elif file.endswith('.json'):
                    with open(file_path, 'r', encoding='utf-8') as f:
                        results['json'] = json.load(f)
            
            return jsonify({
                'status': 'success',
                'results': results
            })
            
        finally:
            # 清理临时文件
            os.unlink(temp_pdf_path)
            import shutil
            shutil.rmtree(output_dir, ignore_errors=True)
            
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
