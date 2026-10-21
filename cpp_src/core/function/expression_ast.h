#pragma once

#include "core/keyvalue/variant.h"
#include "core/payload/fieldsset.h"
#include "core/type_consts.h"
#include "estl/h_vector.h"
#include "estl/tokenizer.h"
#include "tools/assertrx.h"
#include "tools/timetools.h"

#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>

namespace reindexer {

class NamespaceImpl;
class PayloadValue;
class NsContext;
class BuiltinFunction;
template <typename T>
class PayloadIface;

namespace functions {
class FunctionInvoker;
}

/// Scalar result of arithmetic evaluation (WHERE and UPDATE). Empty means a missing/null operand.
struct [[nodiscard]] ExprScalar {
	enum class [[nodiscard]] Kind : uint8_t { Empty, Int, Double };

	Kind kind{Kind::Empty};
	union {
		int64_t i;
		double d;
	};

	ExprScalar() noexcept : i{0} {}
	static ExprScalar FromInt(int64_t v) noexcept {
		ExprScalar result;
		result.kind = Kind::Int;
		result.i = v;
		return result;
	}
	static ExprScalar FromDouble(double v) noexcept {
		ExprScalar result;
		result.kind = Kind::Double;
		result.d = v;
		return result;
	}

	bool empty() const noexcept { return kind == Kind::Empty; }
	static std::optional<ExprScalar> TryFromVariant(const Variant& v) {
		if (v.Type().IsOneOf<KeyValueType::Int, KeyValueType::Int64>()) {
			return FromInt(v.As<int64_t>());
		}
		if (v.Type().IsOneOf<KeyValueType::Double, KeyValueType::Float>()) {
			return FromDouble(v.As<double>());
		}
		return std::nullopt;
	}
	double AsDouble() const {
		switch (kind) {
			case Kind::Int:
				return static_cast<double>(i);
			case Kind::Double:
				return d;
			case Kind::Empty:
				break;
		}
		assertrx_throw(false);
		return 0;
	}
	Variant ToVariant() const {
		switch (kind) {
			case Kind::Int:
				return Variant{i};
			case Kind::Double:
				return Variant{d};
			case Kind::Empty:
				break;
		}
		assertrx_throw(false);
		return {};
	}
};

struct [[nodiscard]] ExprFieldBinding {
	explicit ExprFieldBinding(int idx) : payloadFieldIdx{idx} {}
	ExprFieldBinding(FieldsSet f, KeyValueType type, h_vector<KeyValueType, 4> compositeTypes)
		: fields{std::move(f)}, fieldType{type}, compositeFieldsTypes{std::move(compositeTypes)} {}

	std::optional<int> payloadFieldIdx;
	FieldsSet fields;
	KeyValueType fieldType{KeyValueType::Undefined{}};
	h_vector<KeyValueType, 4> compositeFieldsTypes;
};

/// Evaluation context for arithmetic ExpressionAst (WHERE and UPDATE SET).
struct [[nodiscard]] ExprEvalContext {
	const NamespaceImpl& ns;
	functions::FunctionInvoker* functionInvoker{nullptr};  // required for UPDATE (serial)
	const NsContext* ctx{nullptr};						   // required for UPDATE (serial)
	std::string_view forField;
	bool whereMode{false};
	std::span<const ExprFieldBinding> fieldBindings;
	const NowTimes* nowTimes{nullptr};
	const PayloadIface<const PayloadValue>& payload;
	VariantArray& fieldScratch;
};

enum class [[nodiscard]] ExprBinOp : uint8_t { Add, Sub, Mul, Div, Concat };
enum class [[nodiscard]] ExprNodeType : uint8_t { Number, Field, UnaryMinus, Binary, ArrayLiteral, Function };

struct ExprNode;
using ExprNodePtr = std::unique_ptr<ExprNode>;
using ExprReferencedFields = h_vector<std::string_view, 3>;
using ExprFieldNames = h_vector<std::string, 3>;
using ExprNodeArgs = h_vector<ExprNodePtr, 2>;

struct [[nodiscard]] ExprNode {
	virtual ~ExprNode() = default;
	ExprNodeType Type() const noexcept { return type_; }
	virtual VariantArray Evaluate(ExprEvalContext& ctx) const = 0;
	virtual std::string Dump() const = 0;
	virtual bool UsesNow() const noexcept = 0;
	virtual void CollectReferencedFields(ExprReferencedFields& fields) const = 0;

protected:
	explicit ExprNode(ExprNodeType type) noexcept : type_{type} {}

private:
	ExprNodeType type_;
};

struct [[nodiscard]] ExprNumber final : ExprNode {
	explicit ExprNumber(Variant v) : ExprNode{ExprNodeType::Number}, value{std::move(v)} {}
	VariantArray Evaluate(ExprEvalContext& ctx) const override;
	std::string Dump() const override;
	bool UsesNow() const noexcept override { return false; }
	void CollectReferencedFields(ExprReferencedFields&) const override {}
	Variant value;
};

struct [[nodiscard]] ExprField final : ExprNode {
	ExprField(std::string n, size_t slot, bool quotedName = false)
		: ExprNode{ExprNodeType::Field}, name{std::move(n)}, slot{slot}, quoted{quotedName} {}
	VariantArray Evaluate(ExprEvalContext& ctx) const override;
	ExprScalar EvaluateScalar(ExprEvalContext& ctx) const;
	std::string Dump() const override;
	bool UsesNow() const noexcept override { return false; }
	void CollectReferencedFields(ExprReferencedFields& fields) const override { fields.emplace_back(name); }
	std::string name;
	size_t slot;
	bool quoted{false};

private:
	enum class [[nodiscard]] FieldAccess : uint8_t { Indexed, BindingFields, JsonPath };

	FieldAccess resolveAccess(ExprEvalContext& ctx, int& fieldIdx, const ExprFieldBinding*& binding) const;
	void getByJsonPath(const PayloadIface<const PayloadValue>& pv, const NamespaceImpl& ns, VariantArray& out) const;
	std::string_view jsonPathForEval(const NamespaceImpl& ns) const;
};

struct [[nodiscard]] ExprUnaryMinus final : ExprNode {
	explicit ExprUnaryMinus(ExprNodePtr c) : ExprNode{ExprNodeType::UnaryMinus}, child{std::move(c)} {}
	VariantArray Evaluate(ExprEvalContext& ctx) const override;
	std::string Dump() const override;
	bool UsesNow() const noexcept override { return child && child->UsesNow(); }
	void CollectReferencedFields(ExprReferencedFields& fields) const override {
		if (child) {
			child->CollectReferencedFields(fields);
		}
	}
	ExprNodePtr child;
};

struct [[nodiscard]] ExprBinary final : ExprNode {
	ExprBinary(ExprBinOp o, ExprNodePtr l, ExprNodePtr r)
		: ExprNode{ExprNodeType::Binary}, op{o}, left{std::move(l)}, right{std::move(r)} {}
	VariantArray Evaluate(ExprEvalContext& ctx) const override;
	std::string Dump() const override;
	bool UsesNow() const noexcept override { return (left && left->UsesNow()) || (right && right->UsesNow()); }
	void CollectReferencedFields(ExprReferencedFields& fields) const override {
		if (left) {
			left->CollectReferencedFields(fields);
		}
		if (right) {
			right->CollectReferencedFields(fields);
		}
	}
	ExprBinOp op;
	ExprNodePtr left;
	ExprNodePtr right;
};

struct [[nodiscard]] ExprArrayLiteral final : ExprNode {
	explicit ExprArrayLiteral(VariantArray vs) : ExprNode{ExprNodeType::ArrayLiteral}, values{std::move(vs)} {}
	VariantArray Evaluate(ExprEvalContext& ctx) const override;
	std::string Dump() const override;
	bool UsesNow() const noexcept override { return false; }
	void CollectReferencedFields(ExprReferencedFields&) const override {}
	VariantArray values;
};

struct [[nodiscard]] ExprFunctionArg {
	enum class [[nodiscard]] Kind : uint8_t { Name, QuotedName, String };

	std::string Dump() const;

	std::string value;
	Kind kind;
};
using ExprFunctionArgs = h_vector<ExprFunctionArg, 1>;

struct [[nodiscard]] ExprFunction final : ExprNode {
	enum class [[nodiscard]] Kind : uint8_t { FlatArrayLen, Now, Serial, ArrayRemove, ArrayRemoveOnce };

	ExprFunction(const BuiltinFunction& fn, Kind kind, ExprFunctionArgs stringArgs, TimeUnit timeUnit = TimeUnit::sec);
	ExprFunction(const BuiltinFunction& fn, Kind kind, ExprNodeArgs exprArgs);

	VariantArray Evaluate(ExprEvalContext& ctx) const override;
	ExprScalar EvaluateScalar(ExprEvalContext& ctx) const;
	std::string Dump() const override;
	bool UsesNow() const noexcept override;
	void CollectReferencedFields(ExprReferencedFields& fields) const override;
	bool ReturnsArray() const noexcept;

	Kind kind;
	ExprFunctionArgs stringArgs;
	ExprNodeArgs exprArgs;
	TimeUnit timeUnit{TimeUnit::sec};

private:
	const BuiltinFunction* fn_{nullptr};
};

/// Parsed expression tree. Shared root so QueryArithmeticEntry / ArithmeticComparator stay copyable.
class [[nodiscard]] ExpressionAst {
public:
	ExpressionAst() = default;
	ExpressionAst(ExprNodePtr root, ExprFieldNames fieldNames) : root_{std::move(root)}, fieldNames_{std::move(fieldNames)} {}

	static ExpressionAst Parse(std::string_view expr, bool whereMode);

	VariantArray Evaluate(ExprEvalContext& ctx) const;
	ExprScalar EvaluateScalar(ExprEvalContext& ctx) const;

	bool UsesNow() const noexcept { return root_ && root_->UsesNow(); }
	ExprReferencedFields ReferencedFields() const {
		ExprReferencedFields fields;
		if (root_) {
			root_->CollectReferencedFields(fields);
		}
		return fields;
	}
	const ExprFieldNames& FieldNames() const noexcept { return fieldNames_; }
	const ExprNode* Root() const noexcept { return root_.get(); }

	explicit operator bool() const noexcept { return static_cast<bool>(root_); }

private:
	std::shared_ptr<ExprNode> root_;
	ExprFieldNames fieldNames_;
};

struct [[nodiscard]] ExprParseContext {
	using FunctionArg = ExprFunctionArg;
	using FunctionArgs = ExprFunctionArgs;

	explicit ExprParseContext(std::string_view expr, bool whereMode);

	Tokenizer& Tok() noexcept { return tokenizer_; }
	void SetResult(ExprNodePtr root) { result_ = std::move(root); }
	ExprNodePtr TakeResult() { return std::move(result_); }
	ExprNodePtr MakeField(std::string name, bool quoted = false);
	ExprNodePtr MakeFunction(std::string_view name, ExprNodeArgs args);
	void PushFunctionCall(std::string_view name);
	bool InternFieldNames() const noexcept { return internFieldArgs_.empty() || internFieldArgs_.back() != 0; }
	ExprFieldNames TakeFieldNames() { return std::move(fieldNames_); }
	bool WhereMode() const noexcept { return whereMode_; }
	[[noreturn]] void ThrowError(std::string_view msg) const;

	std::string_view input;
	bool whereMode_{false};

private:
	void PopFunctionCall() noexcept;

	Tokenizer tokenizer_;
	ExprNodePtr result_;
	ExprFieldNames fieldNames_;
	h_vector<uint8_t, 4> internFieldArgs_;
};

}  // namespace reindexer
