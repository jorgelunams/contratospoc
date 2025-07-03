# Contributing to Contract Processing Pipeline

Thank you for your interest in contributing to this project! We welcome contributions from the community.

## Development Setup

1. **Fork the repository**
   ```bash
   git clone https://github.com/jorgelunams/contratospoc.git
   cd contratospoc
   ```

2. **Set up Python environment**
   ```bash
   python -m venv venv
   venv\Scripts\activate  # Windows
   # or
   source venv/bin/activate  # Linux/Mac
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment**
   - Copy `.env.example` to `.env`
   - Fill in your Azure service credentials

## Contribution Guidelines

### Code Style
- Follow PEP 8 Python style guidelines
- Use meaningful variable and function names
- Add docstrings to all functions and classes
- Include type hints where appropriate

### Testing
- Test your changes thoroughly
- Run the debug script: `python contract_processor.py`
- Ensure all existing functionality still works

### Pull Request Process

1. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**
   - Write clean, documented code
   - Add appropriate error handling
   - Update documentation if needed

3. **Commit your changes**
   ```bash
   git add .
   git commit -m "feat: add your feature description"
   ```

4. **Push to your fork**
   ```bash
   git push origin feature/your-feature-name
   ```

5. **Create a Pull Request**
   - Provide a clear description of your changes
   - Reference any related issues
   - Include screenshots if UI changes are involved

### Commit Message Format

Use conventional commit format:
- `feat:` for new features
- `fix:` for bug fixes
- `docs:` for documentation changes
- `refactor:` for code refactoring
- `test:` for adding tests
- `chore:` for maintenance tasks

## Areas for Contribution

- **Error Handling**: Improve error handling and recovery
- **Performance**: Optimize processing speed and memory usage
- **Documentation**: Improve code documentation and examples
- **Testing**: Add unit tests and integration tests
- **Features**: Add new contract analysis capabilities
- **Security**: Enhance security measures and validation

## Code Review Process

1. All submissions require review before merging
2. Reviewers will check for:
   - Code quality and style
   - Functionality and correctness
   - Documentation completeness
   - Security considerations

## Questions?

- Open an issue for bugs or feature requests
- Start a discussion for questions about architecture
- Check existing issues before creating new ones

Thank you for contributing! 🎉
