cargo check && \
 cargo fix --allow-dirty --allow-staged && \
  cargo clippy --no-deps -- \
    --warn clippy::all \
    --warn clippy::pedantic \
    --warn clippy::nursery \
    --warn clippy::cargo \
    --allow clippy::must-use-candidate