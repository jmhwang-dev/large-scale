from optimum.onnxruntime import ORTModelForCausalLM, ORTModelForSequenceClassification
import torch
import os

import os
import torch
import onnxruntime as ort
from optimum.onnxruntime import ORTModelForCausalLM, ORTQuantizer
from optimum.onnxruntime.configuration import AutoQuantizationConfig
from transformers import AutoTokenizer, AutoModelForCausalLM


def translator2onnx():
    onnx_save_path = "infra/consumer/onnx/translate"
    model_path = "infra/consumer/models/translate"
    os.makedirs(onnx_save_path, exist_ok=True)

    # FP16 model load and save to temp path
    fp16_path = "infra/consumer/fp16"
    # 1. float16으로 변환
    # os.makedirs(fp16_path, exist_ok=True)
    # pt_model = AutoModelForCausalLM.from_pretrained(
    #     model_path,
    #     torch_dtype=torch.float16,
    #     trust_remote_code=False
    # )
    # pt_model.save_pretrained(fp16_path)
    # print(f"FP16 model saved to: {fp16_path}")

    # swap 메모리 잠깐 사용

    # # or

    # 양자화
    quantized_path = "infra/consumer/quantized"
    os.makedirs(quantized_path, exist_ok=True)
    quantizer = ORTQuantizer.from_pretrained(fp16_path)
    qconfig = AutoQuantizationConfig.int8(is_static=False)
    quantizer.quantize(save_dir=quantized_path, quantization_config=qconfig)
    print(f"quantized model saved to: {quantized_path}")

    # ONNX conversion from FP16 path
    with torch.no_grad():
        model = ORTModelForCausalLM.from_pretrained(
            fp16_path,
            export=True,
            trust_remote_code=False,
            providers=["CPUExecutionProvider"]  # or GPU if available
        )

    model.save_pretrained(onnx_save_path)
    print(f"ONNX model saved to: {onnx_save_path}")

def sentiment2onnx():
    onnx_save_path = "infra//consumer/onnx/sentiment"
    os.makedirs(onnx_save_path, exist_ok=True)

    model_path = "infra/consumer/models/sentiment"
    model = ORTModelForSequenceClassification.from_pretrained(
        model_path,
        export=True,  # PyTorch -> ONNX 변환 활성화
        trust_remote_code=False,
    )

    model.save_pretrained(onnx_save_path)

if __name__=="__main__":
    # sentiment2onnx()
    translator2onnx()