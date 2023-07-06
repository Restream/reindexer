#pragma once
#include <core/type_consts.h>
#include <memory>
#include "basefunctionctx.h"
#include "core/ft/areaholder.h"
#include "core/ft/usingcontainer.h"
#include "estl/h_vector.h"

namespace reindexer {

class FtCtx : public BaseFunctionCtx {
public:
	typedef std::shared_ptr<FtCtx> Ptr;
	struct Data {
		typedef std::shared_ptr<Data> Ptr;
		std::vector<int16_t> proc_;
		fast_hash_map<IdType, size_t> holders_;
		std::vector<AreaHolder> area_;
		bool need_area_ = false;
		bool is_composite_ = false;
		bool isWordPositions_ = false;
		std::string extraWordSymbols_;
	};

	FtCtx();
	int16_t Proc(size_t pos);
	bool isComposite() const noexcept { return data_->is_composite_; }
	size_t GetSize() const noexcept { return data_->proc_.size(); }

	template <typename InputIterator>
	void Add(InputIterator begin, InputIterator end, int16_t proc, AreaHolder &&holder);
	template <typename InputIterator>
	void Add(InputIterator begin, InputIterator end, int16_t proc);

	template <typename InputIterator>
	void Add(InputIterator begin, InputIterator end, int16_t proc, const std::vector<bool> &mask, AreaHolder &&holder);
	template <typename InputIterator>
	void Add(InputIterator begin, InputIterator end, int16_t proc, const std::vector<bool> &mask);

	void Reserve(size_t size);
	size_t Size() const noexcept;
	bool NeedArea() const noexcept;
	bool PrepareAreas(const RHashMap<std::string, int> &fields, const std::string &name);

	void SetData(Data::Ptr data) noexcept { data_ = std::move(data); }
	const Data::Ptr &GetData() const noexcept { return data_; }

private:
	Data::Ptr data_;
};
}  // namespace reindexer
