# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

if(NOT BUILD_FAISS_GLOBAL)
  message("FAISS target was skipped")
  return()
endif()

set(FAISS_SRC
  AutoTune.cpp
  Clustering.cpp
  Index.cpp
  Index2Layer.cpp
  IndexAdditiveQuantizer.cpp
  IndexBinary.cpp
  IndexBinaryFlat.cpp
  IndexBinaryFromFloat.cpp
  IndexBinaryHNSW.cpp
  IndexBinaryHash.cpp
  IndexBinaryIVF.cpp
  IndexFlat.cpp
  IndexFlatCodes.cpp
  IndexHNSW.cpp
  IndexIDMap.cpp
  IndexIVF.cpp
  IndexIVFAdditiveQuantizer.cpp
  IndexIVFFlat.cpp
  IndexIVFPQ.cpp
  IndexIVFFastScan.cpp
  IndexIVFAdditiveQuantizerFastScan.cpp
  IndexIVFPQFastScan.cpp
  IndexIVFPQR.cpp
  IndexIVFSpectralHash.cpp
  IndexLSH.cpp
  IndexNNDescent.cpp
  IndexLattice.cpp
  IndexNSG.cpp
  IndexPQ.cpp
  IndexFastScan.cpp
  IndexAdditiveQuantizerFastScan.cpp
  IndexIVFIndependentQuantizer.cpp
  IndexPQFastScan.cpp
  IndexPreTransform.cpp
  IndexRefine.cpp
  IndexReplicas.cpp
  IndexRowwiseMinMax.cpp
  IndexScalarQuantizer.cpp
  IndexShards.cpp
  IndexShardsIVF.cpp
  IndexNeuralNetCodec.cpp
  MatrixStats.cpp
  MetaIndexes.cpp
  VectorTransform.cpp
  clone_index.cpp
  index_factory.cpp
  impl/AuxIndexStructures.cpp
  impl/CodePacker.cpp
  impl/IDSelector.cpp
  impl/FaissException.cpp
  impl/HNSW.cpp
  impl/NSG.cpp
  impl/PolysemousTraining.cpp
  impl/ProductQuantizer.cpp
  impl/AdditiveQuantizer.cpp
  impl/ResidualQuantizer.cpp
  impl/LocalSearchQuantizer.cpp
  impl/ProductAdditiveQuantizer.cpp
  impl/ScalarQuantizer.cpp
  impl/index_read.cpp
  impl/index_write.cpp
  impl/io.cpp
  impl/kmeans1d.cpp
  impl/lattice_Zn.cpp
  impl/pq4_fast_scan.cpp
  impl/pq4_fast_scan_search_1.cpp
  impl/pq4_fast_scan_search_qbs.cpp
  impl/residual_quantizer_encode_steps.cpp
  impl/io.cpp
  impl/lattice_Zn.cpp
  impl/NNDescent.cpp
  invlists/BlockInvertedLists.cpp
  invlists/DirectMap.cpp
  invlists/InvertedLists.cpp
  invlists/InvertedListsIOHook.cpp
  utils/Heap.cpp
  utils/NeuralNet.cpp
  utils/WorkerThread.cpp
  utils/distances.cpp
  utils/distances_simd.cpp
  utils/extra_distances.cpp
  utils/hamming.cpp
  utils/partitioning.cpp
  utils/quantize_lut.cpp
  utils/random.cpp
  utils/sorting.cpp
  utils/utils.cpp
  utils/distances_fused/avx512.cpp
  utils/distances_fused/distances_fused.cpp
  utils/distances_fused/simdlib_based.cpp
)

set(FAISS_HEADERS
  AutoTune.h
  Clustering.h
  Index.h
  Index2Layer.h
  IndexAdditiveQuantizer.h
  IndexBinary.h
  IndexBinaryFlat.h
  IndexBinaryFromFloat.h
  IndexBinaryHNSW.h
  IndexBinaryHash.h
  IndexBinaryIVF.h
  IndexFlat.h
  IndexFlatCodes.h
  IndexHNSW.h
  IndexIDMap.h
  IndexIVF.h
  IndexIVFAdditiveQuantizer.h
  IndexIVFIndependentQuantizer.h
  IndexIVFFlat.h
  IndexIVFPQ.h
  IndexIVFFastScan.h
  IndexIVFAdditiveQuantizerFastScan.h
  IndexIVFPQFastScan.h
  IndexIVFPQR.h
  IndexIVFSpectralHash.h
  IndexLSH.h
  IndexLattice.h
  IndexNNDescent.h
  IndexNSG.h
  IndexPQ.h
  IndexFastScan.h
  IndexAdditiveQuantizerFastScan.h
  IndexPQFastScan.h
  IndexPreTransform.h
  IndexRefine.h
  IndexReplicas.h
  IndexRowwiseMinMax.h
  IndexScalarQuantizer.h
  IndexShards.h
  IndexShardsIVF.h
  MatrixStats.h
  MetaIndexes.h
  MetricType.h
  VectorTransform.h
  clone_index.h
  index_factory.h
  index_io.h
  impl/AdditiveQuantizer.h
  impl/AuxIndexStructures.h
  impl/CodePacker.h
  impl/IDSelector.h
  impl/DistanceComputer.h
  impl/FaissAssert.h
  impl/FaissException.h
  impl/HNSW.h
  impl/LocalSearchQuantizer.h
  impl/ProductAdditiveQuantizer.h
  impl/LookupTableScaler.h
  impl/NNDescent.h
  impl/NSG.h
  impl/PolysemousTraining.h
  impl/ProductQuantizer-inl.h
  impl/ProductQuantizer.h
  impl/Quantizer.h
  impl/ResidualQuantizer.h
  impl/ResultHandler.h
  impl/ScalarQuantizer.h
  impl/ThreadedIndex-inl.h
  impl/ThreadedIndex.h
  impl/io.h
  impl/io_macros.h
  impl/kmeans1d.h
  impl/lattice_Zn.h
  impl/platform_macros.h
  impl/pq4_fast_scan.h
  impl/residual_quantizer_encode_steps.h
  impl/simd_result_handlers.h
  impl/code_distance/code_distance.h
  impl/code_distance/code_distance-generic.h
  impl/code_distance/code_distance-avx2.h
  invlists/BlockInvertedLists.h
  invlists/DirectMap.h
  invlists/InvertedLists.h
  invlists/InvertedListsIOHook.h
  utils/AlignedTable.h
  utils/bf16.h
  utils/Heap.h
  utils/WorkerThread.h
  utils/distances.h
  utils/extra_distances-inl.h
  utils/extra_distances.h
  utils/fp16-fp16c.h
  utils/fp16-inl.h
  utils/fp16-arm.h
  utils/fp16.h
  utils/hamming-inl.h
  utils/hamming.h
  utils/ordered_key_value.h
  utils/partitioning.h
  utils/prefetch.h
  utils/quantize_lut.h
  utils/random.h
  utils/sorting.h
  utils/simdlib.h
  utils/simdlib_avx2.h
  utils/simdlib_emulated.h
  utils/simdlib_neon.h
  utils/utils.h
  utils/distances_fused/avx512.h
  utils/distances_fused/distances_fused.h
  utils/distances_fused/simdlib_based.h
  utils/approx_topk/approx_topk.h
  utils/approx_topk/avx2-inl.h
  utils/approx_topk/generic.h
  utils/approx_topk/mode.h
  utils/approx_topk_hamming/approx_topk_hamming.h
  utils/transpose/transpose-avx2-inl.h
  utils/hamming_distance/common.h
  utils/hamming_distance/generic-inl.h
  utils/hamming_distance/hamdis-inl.h
  utils/hamming_distance/neon-inl.h
  utils/hamming_distance/avx2-inl.h
)

if(NOT WIN32)
  list(APPEND FAISS_SRC invlists/OnDiskInvertedLists.cpp)
  list(APPEND FAISS_HEADERS invlists/OnDiskInvertedLists.h)
endif()

# Export FAISS_HEADERS variable to parent scope.
set(FAISS_HEADERS ${FAISS_HEADERS} PARENT_SCOPE)

add_library(faiss_obj OBJECT ${FAISS_HEADERS} ${FAISS_SRC})
message(PROJECT_SOURCE_DIR = ${PROJECT_SOURCE_DIR})
target_include_directories(faiss_obj PUBLIC
  $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>)

#if(WIN32)
#  target_compile_definitions(faiss_obj PRIVATE FAISS_MAIN_LIB)
#endif()

if(MSVC)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -wd4244 -wd4267 -wd4996 -wd4717 -wd4800 -wd4396 -wd4503 -wd4018 -wd4334 -MP -MD /bigobj")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -SAFESEH:NO")
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-switch-enum -Wno-old-style-cast -Wno-sign-compare -Wno-float-equal")
endif()

string(FIND "${CMAKE_CXX_FLAGS}" "FINTEGER" finteger_idx)
if (${finteger_idx} EQUAL -1)
  target_compile_definitions(faiss_obj PRIVATE FINTEGER=int)
endif()

if(FAISS_USE_LTO)
  include(CheckIPOSupported)
  check_ipo_supported(RESULT ipo_supported OUTPUT ipo_error)

  if (ipo_supported)
    message(STATUS "LTO enabled")
    set_property(TARGET faiss_obj PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
  else()
    message(STATUS "LTO not supported: <${ipo_error}>")
  endif()
endif()

if(NOT WITH_TSAN_GLOBAL)
  find_package(OpenMP REQUIRED)
  target_link_libraries(faiss_obj PRIVATE OpenMP::OpenMP_CXX)
  add_definitions(-DFAISS_WITH_OPENMP=1)
else()
  message("TSAN is enabled. Building without OpenMP support...")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unknown-pragmas")
endif()

# TODO: Support dlopen for MKL
#find_package(MKL)
#if(NOT MKL_FOUND)
  #list(APPEND REINDEXER_LIBRARIES ${MKL_LIBRARIES})
#else()
  find_package(BLAS REQUIRED)
  find_package(LAPACK REQUIRED)
#endif()
