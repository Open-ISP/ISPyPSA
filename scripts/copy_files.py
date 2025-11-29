import os
import shutil


def on_pre_build(config):
    # Copy file from root to docs directory
    src = "ispypsa_config.yaml"  # File in root
    dst = os.path.join(config["docs_dir"], "downloads", "ispypsa_config.yaml")

    os.makedirs(os.path.dirname(dst), exist_ok=True)
    shutil.copy2(src, dst)
