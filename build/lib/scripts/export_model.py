import argparse
from pathlib import Path
import torch
from solhunter_zero.models import (
    load_model,
    export_torchscript,
    export_onnx,
    PriceModel,
    TransformerModel,
    DeepLSTMModel,
    DeepTransformerModel,
    XLTransformerModel,
)


def main() -> None:
    p = argparse.ArgumentParser(description="Export a trained model")
    p.add_argument("--model", required=True, help="Path to saved model")
    p.add_argument("--out", required=True, help="Output file path")
    p.add_argument(
        "--format",
        choices=["torchscript", "onnx"],
        default="torchscript",
        help="Export format",
    )
    p.add_argument(
        "--seq-len",
        type=int,
        default=30,
        help="Sequence length for price models when exporting ONNX",
    )
    args = p.parse_args()

    model = load_model(args.model)
    if args.format == "torchscript":
        export_torchscript(model, args.out)
    else:
        if isinstance(
            model,
            (PriceModel, TransformerModel, DeepLSTMModel, DeepTransformerModel, XLTransformerModel),
        ):
            input_dim = (
                model.input_dim if hasattr(model, "input_dim") else model.lstm.input_size
            )
            sample = torch.zeros(1, args.seq_len, input_dim)
        elif hasattr(model, "actor"):
            layer = getattr(model.actor, "0", None)
            if isinstance(layer, torch.nn.Linear):
                sample = torch.zeros(1, layer.in_features)
            else:
                sample = torch.zeros(1, 8)
        elif hasattr(model, "model"):
            layer = getattr(model.model, "0", None)
            if isinstance(layer, torch.nn.Linear):
                sample = torch.zeros(1, layer.in_features)
            else:
                sample = torch.zeros(1, 8)
        else:
            sample = torch.zeros(1, 8)
        export_onnx(model, args.out, sample)


if __name__ == "__main__":  # pragma: no cover
    main()
