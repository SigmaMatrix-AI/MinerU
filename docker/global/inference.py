#!/usr/bin/env python3
import json
import base64
import os
import tempfile
import subprocess
import time
import requests
import logging
from flask import Flask, request, jsonify
from threading import Thread

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# 全局变量存储mineru-api进程
mineru_process = None

def start_mineru_api():
    """启动mineru-api服务"""
    global mineru_process
    try:
        logger.info("Starting mineru-api service...")
        os.environ['MINERU_MODEL_SOURCE'] = 'local'
        
        mineru_process = subprocess.Popen([
            'mineru-api',
            '--host', '0.0.0.0',
            '--port', '8000'  # 使用默认端口8000
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # 等待服务启动
        for i in range(30):  # 等待最多30秒
            try:
                response = requests.get('http://localhost:8000/health', timeout=1)
                if response.status_code == 200:
                    logger.info("mineru-api service started successfully")
                    return True
            except:
                time.sleep(1)
        
        logger.error("Failed to start mineru-api service")
        return False
        
    except Exception as e:
        logger.error(f"Error starting mineru-api: {str(e)}")
        return False

def model_fn(model_dir):
    """SageMaker模型加载函数"""
    logger.info("Loading model...")
    success = start_mineru_api()
    if not success:
        raise Exception("Failed to start mineru-api service")
    return "mineru-api"

def input_fn(request_body, content_type):
    """SageMaker输入处理函数"""
    if content_type == 'application/json':
        input_data = json.loads(request_body)
        return input_data
    else:
        raise ValueError(f"Unsupported content type: {content_type}")

def predict_fn(input_data, model):
    """SageMaker推理函数"""
    try:
        # 获取base64编码的PDF
        pdf_base64 = input_data.get('pdf_base64')
        output_format = input_data.get('output_format', 'markdown')
        
        if not pdf_base64:
            return {"error": "Missing pdf_base64 in request"}
        
        # 解码PDF并保存到临时文件
        pdf_bytes = base64.b64decode(pdf_base64)
        
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp_file:
            tmp_file.write(pdf_bytes)
            pdf_path = tmp_file.name
        
        try:
            # 调用mineru-api服务
            with open(pdf_path, 'rb') as f:
                files = {'file': f}
                data = {'output_format': output_format}
                
                response = requests.post(
                    'http://localhost:8000/v1/extract',
                    files=files,
                    data=data,
                    timeout=300  # 5分钟超时
                )
            
            if response.status_code == 200:
                result = response.json()
                return {
                    "status": "success",
                    "content": result.get('content', ''),
                    "format": output_format,
                    "metadata": result.get('metadata', {})
                }
            else:
                return {
                    "error": f"API call failed with status {response.status_code}: {response.text}"
                }
                
        finally:
            # 清理临时文件
            if os.path.exists(pdf_path):
                os.unlink(pdf_path)
                
    except Exception as e:
        logger.error(f"推理错误: {str(e)}")
        return {"error": str(e)}

def output_fn(prediction, accept):
    """SageMaker输出处理函数"""
    if accept == 'application/json':
        return json.dumps(prediction), accept
    else:
        raise ValueError(f"Unsupported accept type: {accept}")

@app.route('/ping', methods=['GET'])
def ping():
    """SageMaker健康检查接口"""
    try:
        # 检查mineru-api服务是否正常
        response = requests.get('http://localhost:8000/health', timeout=5)
        if response.status_code == 200:
            return jsonify({"status": "healthy"})
        else:
            return jsonify({"status": "unhealthy", "reason": "mineru-api not responding"}), 503
    except Exception as e:
        return jsonify({"status": "unhealthy", "reason": str(e)}), 503

@app.route('/invocations', methods=['POST'])
def invocations():
    """SageMaker推理接口"""
    try:
        # 获取请求数据
        input_data = input_fn(request.data, request.content_type)
        
        # 执行推理
        prediction = predict_fn(input_data, "mineru-api")
        
        # 返回结果
        response, content_type = output_fn(prediction, 'application/json')
        return response, 200, {'Content-Type': content_type}
        
    except Exception as e:
        logger.error(f"请求处理错误: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    # 启动mineru-api服务
    logger.info("Initializing MinerU SageMaker service...")
    model_fn("/opt/ml/model")
    
    # 启动Flask应用
    logger.info("Starting Flask server on port 8080...")
    app.run(host='0.0.0.0', port=8080, debug=False)
